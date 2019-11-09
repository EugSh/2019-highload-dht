package ru.mail.polis.dao.shkalev;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.DAO;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface AdvancedDAO extends DAO {
    Row getRow(@NotNull final ByteBuffer key) throws IOException;
}
