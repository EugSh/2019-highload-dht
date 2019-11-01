package ru.mail.polis.service.shkalev;

import org.jetbrains.annotations.NotNull;

public class Replicas {
    private final int ack;
    private final int from;

    private Replicas(final int ack, final int from) {
        this.ack = ack;
        this.from = from;
    }

    public static Replicas quorum(final int count) {
        return new Replicas(count / 2 + 1, count);
    }

    public static Replicas parse(@NotNull final String replicas) {
        final int iSeparator = replicas.indexOf("/");
        final String ask = replicas.substring(0, iSeparator);
        final String from = replicas.substring(iSeparator + 1);
        return new Replicas(Integer.parseInt(ask), Integer.parseInt(from));
    }

    public int getAck() {
        return ack;
    }

    public int getFrom() {
        return from;
    }
}
