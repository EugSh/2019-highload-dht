package ru.mail.polis.service.shkalev;

import java.util.Collection;

public class NotEnoughReplicasException extends RuntimeException {
    private static final long serialVersionUID = 8645977352730884055L;

    private final Collection<Throwable> errors;

    public NotEnoughReplicasException(Collection<Throwable> errors) {
        this.errors = errors;
    }

    @Override
    public String getMessage() {
        return "NotEnoughReplicasException - " + errors;
    }
}
