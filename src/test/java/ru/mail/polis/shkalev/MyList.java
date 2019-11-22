package ru.mail.polis.shkalev;

public interface MyList<T> {
    boolean add(T t);

    int size();

    T get(int i);
}
