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
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.jetbrains.annotations.NotNull;

import com.google.common.collect.Iterators;

import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;

public class MySuperDAO implements DAO {
    private static final int MODEL = Integer.parseInt(System.getProperty("sun.arch.data.model"));
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

    class Worker extends Thread {
        Worker() {
            super("worker");
        }

        @Override
        public void run() {
            boolean poisoned = false;
            boolean compacting = false;
            while (!poisoned && !isInterrupted()) {
                try {
                    final TableToFlush table = memoryTable.takeToFlush();
                    dump(table.getTable(), table.getFileIndex());
                    compacting = table.isCompacting();
                    if (compacting){
                        final Table compactingTable = Utils.compactFiles(rootDir, tables);
                        tables.clear();
                        fileIndex.set(0);
                        tables.put(fileIndex.get(), compactingTable);
                    }
                    poisoned = table.isPoisonPill();
                    memoryTable.flushed(table.getFileIndex());
                } catch (InterruptedException e) {
                    interrupt();
                } catch (IOException e) {
                    System.err.println("flushing");
                }
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
        this.worker = new Worker();
        this.worker.start();
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        final List<Iterator<Row>> iteratorList = Utils.getListIterators(tables, memoryTable, from);
        return Iterators.transform(Utils.getActualRowIterator(iteratorList), Row::getRecord);
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
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Perform compaction.
     * NotThreadSafe
     */
    @Override
    public void compact() throws IOException {
        memoryTable.compact();
        try {
            worker.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
