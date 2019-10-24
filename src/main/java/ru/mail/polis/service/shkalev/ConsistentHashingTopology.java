package ru.mail.polis.service.shkalev;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Set;
import java.util.TreeSet;

public class ConsistentHashingTopology<T> implements Topology<T> {
    private static final int range = 256;
    public static final int defaultLeft = -range;
    public static final int defaultRight = range;
    private final HashMap<Integer, T> mappedServers;
    private final T me;
    private final Hash hashFunction;

    public ConsistentHashingTopology(@NotNull final Set<T> serversURL,
                                     @NotNull final T me,
                                     final int leftLimit,
                                     final int rightLimit,
                                     @NotNull final Hash hashFunction) {
        assert leftLimit <= rightLimit;
        this.mappedServers = new HashMap<>(rightLimit - leftLimit + 1);
        this.me = me;
        this.hashFunction = hashFunction;
        int offset = 0;
        for (final T server : new TreeSet<>(serversURL)) {
            for (int i = leftLimit + offset; i <= rightLimit; i += serversURL.size()) {
                this.mappedServers.put(i, server);
            }
            offset++;
        }
    }

    public static int defaultHash(Object o) {
        return o.hashCode() % range;
    }

    @Override
    public T primaryFor(@NotNull ByteBuffer key) {
        return mappedServers.get(hashFunction.hash(key));
    }

    @Override
    public boolean isMe(@NotNull T node) {
        return me.equals(node);
    }

    @Override
    public Set<T> all() {
        return new TreeSet<>(mappedServers.values());
    }
}
