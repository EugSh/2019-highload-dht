package ru.mail.polis.service.shkalev;

import one.nio.http.HttpSession;
import one.nio.http.Request;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public interface Replicator {

    void executeGet(@NotNull final HttpSession session, @NotNull final Request request,
                    @NotNull final ByteBuffer key, final boolean isProxy, @NotNull final Replicas rf);

    void executePut(@NotNull final HttpSession session, @NotNull final Request request,
                    @NotNull final ByteBuffer key, final boolean isProxy, @NotNull final Replicas rf);

    void executeDelete(@NotNull final HttpSession session, @NotNull final Request request,
                       @NotNull final ByteBuffer key, final boolean isProxy, @NotNull final Replicas rf);
}
