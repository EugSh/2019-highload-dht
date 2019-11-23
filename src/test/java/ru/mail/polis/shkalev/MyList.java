package ru.mail.polis.shkalev;

interface MyList<T> {
    boolean add(T t);

    int size();

    T get(int i);
}
