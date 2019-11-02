package ru.mail.polis.shkalev;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import ru.mail.polis.service.shkalev.Replicas;
import ru.mail.polis.service.shkalev.Ring;
import ru.mail.polis.service.shkalev.Topology;

import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class RingTest {

    @Test
    @RepeatedTest(1000)
    public void distribution() {
        uniformDistribution(8, 1_000, 1, 3, 0.15f);
        uniformDistribution(8, 10_000, 3, 3, 0.1f);
        uniformDistribution(8, 100_000, 5, 7, 0.05f);
    }

    @Test
    @RepeatedTest(1000)
    public void distributionRF() {
        uniformDistributionRF(8, 1_000, 1, 3, 0.15f, Replicas.parse("1/1"));
        uniformDistributionRF(8, 10_000, 3, 3, 0.1f, Replicas.parse("1/2"));
        uniformDistributionRF(8, 100_000, 5, 7, 0.05f, Replicas.parse("1/4"));
    }

    private void uniformDistributionRF(final int keyLen, final int keyCount, final int nodeCount, final int duplicateFactor, final float epsilon, final Replicas rf) {
        final Topology<String> ring = createTopology(nodeCount, duplicateFactor);
        final Map<String, Integer> counter = new HashMap<>();
        final Random random = new Random();
        for (int i = 0; i < keyCount; i++) {
            final byte[] key = new byte[keyLen];
            random.nextBytes(key);
            final Set<String> nodes = ring.primaryFor(ByteBuffer.wrap(key), rf);
            for (final String node : nodes) {
                counter.compute(node, (k, v) -> v == null ? 1 : v + 1);
            }
        }
        final int meanCount = keyCount / nodeCount * rf.getFrom();

        counter.entrySet().stream().forEach(elem -> {
            assertTrue(Math.abs(elem.getValue() - meanCount) < (int) (meanCount * epsilon));
        });
    }

    private void uniformDistribution(final int keyLen, final int keyCount, final int nodeCount, final int duplicateFactor, final float epsilon) {
        final Topology<String> ring = createTopology(nodeCount, duplicateFactor);
        final Map<String, Integer> counter = new HashMap<>();
        final Random random = new Random();
        for (int i = 0; i < keyCount; i++) {
            final byte[] key = new byte[keyLen];
            random.nextBytes(key);
            final String node = ring.primaryFor(ByteBuffer.wrap(key));
            counter.compute(node, (k, v) -> v == null ? 1 : v + 1);
        }
        final int meanCount = keyCount / nodeCount;

        counter.entrySet().stream().forEach(elem -> {
            assertTrue(Math.abs(elem.getValue() - meanCount) < (int) (meanCount * epsilon));
        });
    }

    private Topology<String> createTopology(final int nodeCount, final int duplicateFactor) {
        final Set<String> nodes = new HashSet<>();
        final String me = "node_0";
        nodes.add(me);
        for (int i = 1; i < nodeCount; i++) {
            nodes.add("node_" + i);
        }
        return new Ring(nodes, me, duplicateFactor);
    }
}
