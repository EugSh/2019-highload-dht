package ru.mail.polis.service.shkalev;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class Ring implements Topology<Address> {
    private final int[] leftBorder;
    private final int[] nodeIndexes;
    private final Address[] nodes;
    private final Address myNode;

    /**
     * Ring for consistent hashing.
     *
     * @param servers         set of all node servers.
     * @param me              own server node.
     * @param duplicateFactor the number of copies of each node.
     */
    public Ring(@NotNull final Set<String> servers,
                @NotNull final String me,
                final int duplicateFactor) {
        final int countNodes = duplicateFactor * servers.size();
        nodes = new Address[servers.size()];
        leftBorder = new int[countNodes];
        nodeIndexes = new int[countNodes];
        myNode = new Address(me);
        servers.stream().map(Address::new).collect(Collectors.toList()).toArray(nodes);
        Arrays.sort(this.nodes);
        final int step = (int) (((long) Integer.MAX_VALUE - (long) Integer.MIN_VALUE + 1) / countNodes);
        for (int i = 0; i < leftBorder.length; i++) {
            leftBorder[i] = Integer.MIN_VALUE + i * step;
            nodeIndexes[i] = i % servers.size();
        }
    }

    @Override
    public Address primaryFor(final @NotNull ByteBuffer key) {
        return nodes[nodeIndexes[binSearch(leftBorder, key.hashCode())]];
    }

    @Override
    public Set<Address> primaryFor(@NotNull final ByteBuffer key, @NotNull final Replicas replicas) {
        final Set<Address> result = new HashSet<>();
        int startI = binSearch(leftBorder, key.hashCode());
        while (result.size() < replicas.getFrom()) {
            result.add(nodes[nodeIndexes[startI]]);
            startI++;
            if (startI == nodeIndexes.length) {
                startI = 0;
            }
        }
        return result;
    }

    @Override
    public boolean isMe(final @NotNull Address node) {
        return myNode.equals(node);
    }

    @Override
    public Set<Address> all() {
        return Set.of(nodes);
    }

    @Override
    public int size() {
        return nodes.length;
    }

    private int binSearch(final int[] array, final int key) {
        int left = 0;
        int right = array.length - 1;
        while (left < right) {
            final int mid = left + (right - left) / 2;
            if (array[mid] <= key) {
                if (array[mid + 1] > key) {
                    return mid;
                } else {
                    left = mid + 1;
                }
            } else {
                right = mid - 1;
            }
        }
        return left;
    }
}
