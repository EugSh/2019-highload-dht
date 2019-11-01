package ru.mail.polis.service.shkalev;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Set;

public interface Topology<T> {
    T primaryFor(@NotNull final ByteBuffer key);

    Set<T> primaryFor(@NotNull final ByteBuffer key,
                      @NotNull final Replicas replicas);

    boolean isMe(@NotNull final T node);

    Set<T> all();

    int size();
}
