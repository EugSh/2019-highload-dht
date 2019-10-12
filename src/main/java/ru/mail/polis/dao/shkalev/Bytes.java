package ru.mail.polis.dao.shkalev;

import java.nio.ByteBuffer;

final class Bytes {
    private Bytes() {
        // Not instantiatable
    }

    static ByteBuffer fromInt(final int i) {
        return ByteBuffer.allocate(Integer.BYTES).putInt(i).rewind();
    }
}
