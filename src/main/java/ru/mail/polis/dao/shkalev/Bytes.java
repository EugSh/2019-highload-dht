package ru.mail.polis.dao.shkalev;

import java.nio.ByteBuffer;

final class Bytes {
    private Bytes() {
        // Not instantiatable
    }

    static ByteBuffer fromInt(final int i) {
        return ByteBuffer.allocate(Integer.BYTES).putInt(i).rewind();
    }

    static ByteBuffer fromLong(final long i) {
        return ByteBuffer.allocate(Long.BYTES).putLong(i).rewind();
    }
}
