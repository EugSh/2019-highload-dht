package ru.mail.polis.shkalev;

import java.util.LinkedList;

public class FixedQueue<T> implements MyList<T> {
    private final int maxSize;
    private final LinkedList<T> delegate;

    private FixedQueue(final int maxSize) {
        this.delegate = new LinkedList<>();
        this.maxSize = maxSize;
    }

    static <T> FixedQueue<T> create(final int maxSize) {
        return new FixedQueue<T>(maxSize);
    }

    @Override
    public boolean add(T t) {
        if (maxSize == 0) {
            return true;
        }
        if (size() == maxSize) {
            delegate.remove();
        }
        delegate.add(t);
        return true;
    }

    @Override
    public int size() {
        return delegate.size();
    }

    @Override
    public T get(int i) {
        return delegate.get(i);
    }


}
