package ru.mail.polis.dao.shkalev;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class MemoryTable implements Table {
    private final SortedMap<ByteBuffer, Row> memTable = new ConcurrentSkipListMap<>();
    private final AtomicLong currentHeap = new AtomicLong(0);

    @NotNull
    @Override
    public Iterator<Row> iterator(@NotNull ByteBuffer from) throws IOException {
        return memTable.tailMap(from).values().iterator();
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value,
                       @NotNull AtomicInteger fileIndex) throws IOException {
        final Row previousRow = memTable.put(key, Row.of(fileIndex.get(), key, value, MySuperDAO.ALIVE));
        if (previousRow == null) {
            currentHeap.addAndGet(Integer.BYTES
                    + (long) (key.remaining() + MySuperDAO.LINK_SIZE + Integer.BYTES * MySuperDAO.NUMBER_FIELDS_BYTEBUFFER)
                    + (long) (value.remaining() + MySuperDAO.LINK_SIZE + Integer.BYTES * MySuperDAO.NUMBER_FIELDS_BYTEBUFFER)
                    + Integer.BYTES);
        } else {
            currentHeap.addAndGet(value.remaining() - previousRow.getValue().remaining());
        }
    }

    @Override
    public void remove(@NotNull ByteBuffer key,
                       @NotNull AtomicInteger fileIndex) throws IOException {
        final Row removedRow = memTable.put(key, Row.of(fileIndex.get(), key, MySuperDAO.TOMBSTONE, MySuperDAO.DEAD));
        if (removedRow == null) {
            currentHeap.addAndGet(Integer.BYTES
                    + (long) (key.remaining() + MySuperDAO.LINK_SIZE + Integer.BYTES * MySuperDAO.NUMBER_FIELDS_BYTEBUFFER)
                    + (long) (MySuperDAO.LINK_SIZE + Integer.BYTES * MySuperDAO.NUMBER_FIELDS_BYTEBUFFER)
                    + Integer.BYTES);
        } else if (!removedRow.isDead()) {
            currentHeap.addAndGet(-removedRow.getValue().remaining());
        }
    }

    @Override
    public void clear() {
        memTable.clear();
        currentHeap.set(0);
    }

    @Override
    public long sizeInBytes() {
        return currentHeap.get();
    }
}
