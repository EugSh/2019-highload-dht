package ru.mail.polis.dao.shkalev;

import java.nio.ByteBuffer;

import org.jetbrains.annotations.NotNull;

import ru.mail.polis.Record;

public final class Row implements Comparable<Row> {
    private final int index;
    private final ByteBuffer key;
    private final ByteBuffer value;
    private final long time;
    private final int status;

    public long getTime() {
        return time;
    }

    private Row(final int index,
                @NotNull final ByteBuffer key,
                @NotNull final ByteBuffer value,
                final int status,
                final long time) {
        this.index = index;
        this.key = key;
        this.value = value;
        this.status = status;
        this.time = time;
    }

    public static Row of(final int index,
                         @NotNull final ByteBuffer key,
                         @NotNull final ByteBuffer value,
                         final int status,
                         final long time) {
        return new Row(index, key, value, status, time);
    }

    public static Row of(final int index,
                         @NotNull final ByteBuffer key,
                         @NotNull final ByteBuffer value,
                         final int status) {
        return new Row(index, key, value, status, Utils.currentTimeNanos());
    }

    public Row copy() {
        return new Row(index,
                key.duplicate().asReadOnlyBuffer(),
                value.duplicate().asReadOnlyBuffer(),
                status,
                time);
    }

    /**
     * Creates an object of class Record.
     *
     * @return Record
     */
    public Record getRecord() {
        if (isDead()) {
            return Record.of(key, MySuperDAO.TOMBSTONE);
        } else {
            return Record.of(key, value);
        }
    }

    public boolean isDead() {
        return status == MySuperDAO.DEAD;
    }

    public ByteBuffer getKey() {
        return key.asReadOnlyBuffer();
    }

    public ByteBuffer getValue() {
        return value.asReadOnlyBuffer();
    }

    public int getIndex() {
        return index;
    }

    @Override
    public int compareTo(@NotNull final Row o) {
        if (key.compareTo(o.getKey()) == 0) {
            return -Integer.compare(index, o.getIndex());
        }
        return key.compareTo(o.getKey());
    }
}
