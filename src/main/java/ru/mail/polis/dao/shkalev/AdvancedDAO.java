package ru.mail.polis.dao.shkalev;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

public interface AdvancedDAO extends DAO {
    Row getRow(@NotNull final ByteBuffer key) throws IOException;
}
