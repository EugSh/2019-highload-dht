package ru.mail.polis.dao.shkalev;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

import org.jetbrains.annotations.NotNull;

class FileTable implements Table{
    private final int count;
    private final int fileIndex;
    private final ByteBuffer rows;
    private final IntBuffer offsets;
    private final File file;

    /**
     * Creates an object that is a file on disk, with the ability to create an iterator on this file.
     *
     * @param file file for which you need to get a table
     * @throws IOException if an I/O error is thrown by a read method
     */
    FileTable(@NotNull final File file) throws IOException {
        this.file = file;
        this.fileIndex = Integer.parseInt(file
                .getName()
                .substring(MySuperDAO.PREFIX.length(), file.getName().length() - MySuperDAO.SUFFIX.length()));
        try (FileChannel fc = FileChannel.open(file.toPath(),
                StandardOpenOption.READ)) {
            final var mapped = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size())
                    .order(ByteOrder.BIG_ENDIAN);
            this.count = mapped.getInt(mapped.limit() - Integer.BYTES);
            final var offsetsBuffer = mapped.duplicate()
                    .position(mapped.limit() - Integer.BYTES - Integer.BYTES * count)
                    .limit(mapped.limit() - Integer.BYTES);
            this.offsets = offsetsBuffer.slice().asIntBuffer();

            this.rows = mapped.asReadOnlyBuffer()
                    .limit(offsetsBuffer.position())
                    .slice();
        }
    }

    /**
     * Creates file iterator.
     *
     * @param from the key from which the iterator will begin
     * @return file iterator
     * @throws IOException if an I/O error is thrown by a read method
     */
    @NotNull
    public Iterator<Row> iterator(@NotNull final ByteBuffer from) throws IOException {
        return new Iterator<>() {
            int index = getOffsetsIndex(from);

            @Override
            public boolean hasNext() {
                return index < count;
            }

            @Override
            public Row next() {
                assert hasNext();
                Row row = null;
                try {
                    row = getRowAt(index++);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return row;
            }
        };
    }

    @Override
    public void upsert(@NotNull ByteBuffer key,
                       @NotNull ByteBuffer value,
                       @NotNull AtomicInteger fileIndex) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void remove(@NotNull ByteBuffer key,
                       @NotNull AtomicInteger fileIndex) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        try {
            Files.delete(file.toPath());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public long sizeInBytes() {
        return file.length();
    }

    private int getOffsetsIndex(@NotNull final ByteBuffer from) throws IOException {
        int left = 0;
        int right = count - 1;
        while (left <= right) {
            final int middle = left + (right - left) / 2;
            final int resCmp = from.compareTo(getKeyAt(middle));
            if (resCmp < 0) {
                right = middle - 1;
            } else if (resCmp > 0) {
                left = middle + 1;
            } else {
                return middle;
            }
        }
        return left;
    }

    private ByteBuffer getKeyAt(final int i) throws IOException {
        assert 0 <= i && i < count;
        final int offset = offsets.get(i);
        final int keySize = rows.getInt(offset);
        return rows.duplicate().position(offset + Integer.BYTES).limit(offset + Integer.BYTES + keySize).slice();
    }

    private Row getRowAt(final int i) throws IOException {
        assert 0 <= i && i < count;
        int offset = offsets.get(i);

        //Key
        final ByteBuffer keyBB = getKeyAt(i);
        offset += Integer.BYTES + keyBB.remaining();

        //Status
        final int status = rows.getInt(offset);
        offset += Integer.BYTES;

        if (status == MySuperDAO.DEAD) {
            return Row.of(fileIndex, keyBB, MySuperDAO.TOMBSTONE, status);
        } else {
            //Value
            final int valueSize = rows.getInt(offset);
            final ByteBuffer valueBB = rows.duplicate().position(offset + Integer.BYTES)
                    .limit(offset + Integer.BYTES + valueSize)
                    .slice();
            return Row.of(fileIndex, keyBB, valueBB, status);
        }
    }
}
