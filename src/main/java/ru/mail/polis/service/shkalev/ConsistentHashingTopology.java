package ru.mail.polis.service.shkalev;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class ConsistentHashingTopology<T> implements Topology<T> {
    private static final int RANGE = 256;
    public static final int DEFAULT_LEFT = -RANGE;
    public static final int DEFAULT_RIGHT = RANGE;
    private final Map<Integer, T> mappedServers;
    private final T me;
    private final Hash hashFunction;

    /**
     * Topology for cluster based on consistent hashing.
     *
     * @param serversURL   URL fro all servers in cluster.
     * @param me           myself server URL in cluster.
     * @param leftLimit    left border of hash table.
     * @param rightLimit   right border of hash table.
     * @param hashFunction function for hashing.
     */
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

    public static int defaultHash(@NotNull final Object o) {
        return o.hashCode() % RANGE;
    }

    @Override
    public T primaryFor(@NotNull final ByteBuffer key) {
        return mappedServers.get(hashFunction.hash(key));
    }

    @Override
    public boolean isMe(@NotNull final T node) {
        return me.equals(node);
    }

    @Override
    public Set<T> all() {
        return new TreeSet<>(mappedServers.values());
    }
}
