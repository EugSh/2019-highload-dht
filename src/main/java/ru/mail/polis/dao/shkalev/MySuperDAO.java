package ru.mail.polis.dao.shkalev;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.jetbrains.annotations.NotNull;

import com.google.common.collect.Iterators;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Record;

public class MySuperDAO implements AdvancedDAO {
    private static final int MODEL = Integer.parseInt(System.getProperty("sun.arch.data.model"));
    private static final Logger log = LoggerFactory.getLogger(MySuperDAO.class);
    private final MemoryTablePool memoryTable;
    private final File rootDir;
    private final AtomicInteger fileIndex = new AtomicInteger(0);
    private final NavigableMap<Integer, Table> tables;
    private final Worker worker;

    static final ByteBuffer TOMBSTONE = ByteBuffer.allocate(0);
    static final int ALIVE = 1;
    static final int DEAD = 0;
    static final int LINK_SIZE = MODEL == 64 ? 8 : 4;
    static final int NUMBER_FIELDS_BYTEBUFFER = 7;
    static final ByteBuffer LEAST_KEY = ByteBuffer.allocate(0);
    static final String PREFIX = "FT";
    static final String SUFFIX = ".mydb";

    @Override
    public Row getRow(@NotNull final ByteBuffer key) throws IOException {
        final Row row = rowBy(key);
        if (row == null) {
            throw new NoSuchElementExceptionLite("Not found");
        }
        return row;
    }

    private Row rowBy(@NonNull final ByteBuffer key) throws IOException {
        final Iterator<Row> iter = rowIterator(key);
        Row row = null;
        if (iter.hasNext()) {
            row = iter.next();
            row = row.getKey().equals(key) ? row : null;
        }
        return row;
    }

    class Worker extends Thread {
        private final ExecutorService executor;

        Worker(final int number) {
            super("Flusher-" + number);
            executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(),
                    new ThreadFactoryBuilder().setNameFormat("asyncFlusher").build());
        }

        @Override
        public void run() {
            AtomicBoolean poisoned = new AtomicBoolean(false);
            while (poisoned.compareAndSet(false, false) && !isInterrupted()) {
                try {
                    final TableToFlush table = memoryTable.takeToFlush();
                    poisoned.set(table.isPoisonPill());
                    executor.execute(() -> {
                        try {
                            final boolean compacting = table.isCompacting();
                            dump(table.getTable(), table.getFileIndex());
                            log.info("dump");
                            if (compacting) {
                                final Table compactingTable = Utils.compactFiles(rootDir, tables);
                                tables.clear();
                                fileIndex.set(Utils.START_FILE_INDEX + 1);
                                tables.put(fileIndex.get(), compactingTable);
                                memoryTable.compacted();
                            }
                            memoryTable.flushed(table.getFileIndex());
                        } catch (IOException e) {
                            log.error("IOException during flushing file", e);
                        }

                    });
                } catch (InterruptedException e) {
                    log.error("InterruptedException during flushing file", e);
                    interrupt();
                }
            }
            executor.shutdown();
            try {
                executor.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                log.error("InterruptedException during termination executor for flushing", e);
            }
        }
    }

    /**
     * Creates LSM storage.
     *
     * @param maxHeap threshold of size of the memTable
     * @param rootDir the folder in which files will be written and read
     * @throws IOException if an I/O error is thrown by a File walker
     */
    public MySuperDAO(final long maxHeap, @NotNull final File rootDir) throws IOException {
        this.rootDir = rootDir;
        this.tables = new ConcurrentSkipListMap<>();
        final EnumSet<FileVisitOption> options = EnumSet.of(FileVisitOption.FOLLOW_LINKS);
        final int maxDeep = 1;
        Files.walkFileTree(rootDir.toPath(), options, maxDeep, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
                if (file.getFileName().toString().startsWith(PREFIX)
                        && file.getFileName().toString().endsWith(SUFFIX)) {
                    final Table fileTable = new FileTable(new File(rootDir, file.getFileName().toString()));
                    tables.put(fileIndex.getAndAdd(1), fileTable);
                }
                return FileVisitResult.CONTINUE;
            }
        });
        this.memoryTable = new MemoryTablePool(maxHeap, fileIndex);
        this.worker = new Worker(1);
        this.worker.start();
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        return Iterators.transform(Utils.aliveRowIterators(rowIterator(from)), Row::getRecord);
    }

    private Iterator<Row> rowIterator(@NotNull final ByteBuffer from) throws IOException {
        final List<Iterator<Row>> iteratorList = Utils.getListIterators(tables, memoryTable, from);
        return Utils.getActualRowIterator(iteratorList);
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        memoryTable.upsert(key, value, fileIndex);
    }

    private void dump(@NotNull final Table table, final int fileIndex) throws IOException {
        final String fileTableName = PREFIX + fileIndex + SUFFIX;
        final File file = new File(rootDir, fileTableName);
        Utils.write(file, table.iterator(LEAST_KEY));
        tables.put(fileIndex, new FileTable(file));
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        memoryTable.remove(key, fileIndex);
    }

    @Override
    public void close() throws IOException {
        memoryTable.close();
        try {
            worker.join();
        } catch (InterruptedException e) {
            log.error("InterruptedException during dao close", e);
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Perform compaction.
     */
    @Override
    public void compact() throws IOException {
        memoryTable.compact();
    }
}
