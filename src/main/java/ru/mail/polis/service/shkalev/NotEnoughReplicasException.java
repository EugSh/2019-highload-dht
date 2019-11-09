package ru.mail.polis.service.shkalev;

import org.jetbrains.annotations.NotNull;

import java.util.Collection;

public class NotEnoughReplicasException extends RuntimeException {
    private static final long serialVersionUID = 8645977352730884055L;

    private final Collection<Throwable> errors;

    NotEnoughReplicasException(@NotNull final Collection<Throwable> errors) {
        this.errors = errors;
    }

    @Override
    public String getMessage() {
        return "NotEnoughReplicasException - " + errors;
    }
}
