package ru.mail.polis.shkalev;

import java.util.ArrayList;
import java.util.List;

class ArrayMyList<T> implements MyList<T> {
    private final List<T> delegate;

    private ArrayMyList(final int size) {
        delegate = new ArrayList<>(size);
    }

    static <T> ArrayMyList<T> create(final int initSize) {
        return new ArrayMyList<T>(initSize);
    }

    @Override
    public boolean add(T t) {
        return delegate.add(t);
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
