package ru.mail.polis.service.shkalev;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Set;

public class Ring implements Topology<String> {
    private final int[] leftBorder;
    private final int[] nodeIndexes;
    private final String[] nodes;
    private final String myNode;

    public Ring(@NotNull final Set<String> servers,
                @NotNull final String me,
                final int duplicateFactor) {
        int countNodes = duplicateFactor * servers.size();
        nodes = new String[servers.size()];
        leftBorder = new int[countNodes];
        nodeIndexes = new int[countNodes];
        myNode = me;
        servers.toArray(this.nodes);
        Arrays.sort(this.nodes);
        final int step = (int) (((long) Integer.MAX_VALUE - (long) Integer.MIN_VALUE + 1) / countNodes);
        for (int i = 0; i < leftBorder.length; i++) {
            leftBorder[i] = Integer.MIN_VALUE + i * step;
            nodeIndexes[i] = i % servers.size();
        }
    }

    @Override
    public String primaryFor(@NotNull ByteBuffer key) {
        return nodes[nodeIndexes[binSearch(leftBorder, key.hashCode())]];
    }

    @Override
    public boolean isMe(@NotNull String node) {
        return myNode.equals(node);
    }

    @Override
    public Set<String> all() {
        return Set.of(nodes);
    }

    private int binSearch(int[] array, int key) {
        int left = 0;
        int right = array.length - 1;
        while (left < right) {
            int mid = left + (right - left) / 2;
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