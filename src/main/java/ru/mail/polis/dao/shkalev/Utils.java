package ru.mail.polis.dao.shkalev;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.*;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.Iters;

final class Utils {
    private static final String TMP = ".tmp";
    private static final int START_FILE_INDEX = 0;
    private static final ByteBuffer LEAST_KEY = MySuperDAO.LEAST_KEY;

    private Utils() {
    }

    /**
     * Compact files. Since deletions and changes accumulate, we have to collapse
     * all these changes, on the one hand, reducing the search time, on the
     * other - reducing the required storage space. Single file will be created
     * in which the most relevant data will be stored and the rest will be deleted.
     *
     * @param rootDir    base directory
     * @param fileTables list file tables that will collapse
     * @throws IOException if an I/O error is thrown by FileTable.iterator
     */
    static Table compactFiles(@NotNull final File rootDir,
                              @NotNull final NavigableMap<Integer, Table> fileTables) throws IOException {
        final List<Iterator<Row>> tableIterators = new LinkedList<>();
        for (final Table fileT : fileTables.values()) {
            tableIterators.add(fileT.iterator(LEAST_KEY));
        }
        final Iterator<Row> filteredRow = getActualRowIterator(tableIterators);
        final File compactFileTmp = compact(rootDir, filteredRow);
        for (final Map.Entry<Integer, Table> entry :
                fileTables.entrySet()) {
            entry.getValue().clear();
        }
        final String fileDbName = MySuperDAO.PREFIX + START_FILE_INDEX + MySuperDAO.SUFFIX;
        final File compactFileDb = new File(rootDir, fileDbName);
        Files.move(compactFileTmp.toPath(), compactFileDb.toPath(), StandardCopyOption.ATOMIC_MOVE);
        return new FileTable(compactFileDb);
    }

    private static File compact(@NotNull final File rootDir,
                                @NotNull final Iterator<Row> rows) throws IOException {
        final String fileTableName = MySuperDAO.PREFIX + START_FILE_INDEX + TMP;
        final File table = new File(rootDir, fileTableName);
        Utils.write(table, rows);
        return table;
    }

    /**
     * Writes data to file. First writes all row: key length, key, status (DEAD, ALIVE),
     * value length, value. Then writes offsets array, and then writes amount of row.
     *
     * @param to   file being recorded
     * @param rows strings to be written to file
     * @throws IOException if an I/O error is thrown by a write method
     */
    static void write(@NotNull final File to,
                      @NotNull final Iterator<Row> rows) throws IOException {
        try (FileChannel fileChannel = FileChannel.open(to.toPath(),
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE)) {
            final List<Integer> offsets = writeRows(fileChannel, rows);
            writeOffsets(fileChannel, offsets);
            fileChannel.write(Bytes.fromInt(offsets.size()));
        }
    }

    private static int writeByteBuffer(@NotNull final FileChannel fc,
                                       @NotNull final ByteBuffer buffer) throws IOException {
        int offset = 0;
        offset += fc.write(Bytes.fromInt(buffer.remaining()));
        offset += fc.write(buffer);
        return offset;
    }

    private static void writeOffsets(@NotNull final FileChannel fc,
                                     @NotNull final List<Integer> offsets) throws IOException {
        for (final Integer elemOffSets : offsets) {
            fc.write(Bytes.fromInt(elemOffSets));
        }
    }

    private static List<Integer> writeRows(@NotNull final FileChannel fc,
                                           @NotNull final Iterator<Row> rows) throws IOException {
        final List<Integer> offsets = new ArrayList<>();
        int offset = 0;
        while (rows.hasNext()) {
            offsets.add(offset);
            final Row row = rows.next();

            //Key
            offset += writeByteBuffer(fc, row.getKey());

            //Value
            if (row.isDead()) {
                offset += fc.write(Bytes.fromInt(MySuperDAO.DEAD));
            } else {
                offset += fc.write(Bytes.fromInt(MySuperDAO.ALIVE));
                offset += writeByteBuffer(fc, row.getValue()); // row.getValue().getData()
            }
        }
        return offsets;
    }

    /**
     * Get merge sorted, collapse equals, without dead row iterator.
     *
     * @param tableIterators collection MyTableIterator
     * @return Row Iterator
     */
    static Iterator<Row> getActualRowIterator(@NotNull final Collection<Iterator<Row>> tableIterators) {
        final Iterator<Row> mergingTableIterator = Iterators.mergeSorted(tableIterators, Row::compareTo);
        final Iterator<Row> collapsedIterator = Iters.collapseEquals(mergingTableIterator, Row::getKey);
        return Iterators.filter(collapsedIterator, row -> !row.isDead());
    }

    static List<Iterator<Row>> getListIterators(@NotNull final NavigableMap<Integer, Table> tables,
                                                @NotNull final Table memTable,
                                                @NotNull final ByteBuffer from) throws IOException {
        final List<Iterator<Row>> tableIterators = new LinkedList<>();
        for (final Table fileT : tables.descendingMap().values()) {
            tableIterators.add(fileT.iterator(from));
        }
        final Iterator<Row> memTableIterator = memTable.iterator(from);
        tableIterators.add(memTableIterator);
        return tableIterators;
    }
}
