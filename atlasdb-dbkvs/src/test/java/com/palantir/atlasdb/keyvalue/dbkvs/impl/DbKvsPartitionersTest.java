package com.palantir.atlasdb.keyvalue.dbkvs.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.keyvalue.dbkvs.util.DbKvsPartitioners;

public class DbKvsPartitionersTest {
    @Test
    public void testBasic() {
        Map<Integer, Integer> counts = ImmutableMap.of(0, 5,
                                                       1, 1,
                                                       2, 2,
                                                       3, 2);
        List<Map<Integer, Integer>> partitioned = DbKvsPartitioners.partitionByTotalCount(counts, 5);
        Assert.assertEquals(2, partitioned.size());
        Assert.assertEquals(ImmutableMap.of(0, 5), partitioned.get(0));
        Assert.assertEquals(ImmutableMap.of(1, 1,
                                            2, 2,
                                            3, 2),
                            partitioned.get(1));
    }

    @Test
    public void testKeySplitAcrossPartitions() {
        Map<Integer, Integer> counts = ImmutableMap.of(0, 3,
                                                       3, 3,
                                                       1, 3,
                                                       4, 3);
        List<Map<Integer, Integer>> partitioned = DbKvsPartitioners.partitionByTotalCount(counts, 5);
        Assert.assertEquals(3, partitioned.size());
        Assert.assertEquals(ImmutableMap.of(0, 3,
                                            3, 2),
                            partitioned.get(0));
        Assert.assertEquals(ImmutableMap.of(3, 1,
                                            1, 3,
                                            4, 1),
                            partitioned.get(1));
        Assert.assertEquals(ImmutableMap.of(4, 2), partitioned.get(2));
    }

    @Test
    public void testKeyWithLargeCount() {
        Map<Integer, Integer> counts = ImmutableMap.of(0, 1,
                                                       1, 99);
        List<Map<Integer, Integer>> partitioned = DbKvsPartitioners.partitionByTotalCount(counts, 5);
        Assert.assertEquals(20, partitioned.size());
        Assert.assertEquals(ImmutableMap.of(0, 1,
                                            1, 4),
                            partitioned.get(0));
        for (int i = 1; i < 20; i++) {
            Assert.assertEquals(ImmutableMap.of(1, 5), partitioned.get(i));
        }
    }

    @Test
    public void testLarge() {
        Random random = new Random(0);

        LinkedHashMap<Integer, Integer> counts = new LinkedHashMap<>();
        for (int i = 0; i < 1000; i++) {
            int key;
            do {
                key = random.nextInt();
            } while (counts.containsKey(key));
            counts.put(key, random.nextInt(1000));
        }

        int partitionSize = 234;
        List<Map<Integer, Integer>> partitioned = DbKvsPartitioners.partitionByTotalCount(counts, partitionSize);
        // All partitions except the last should be completely filled
        for (Map<Integer, Integer> partition : partitioned.subList(0, partitioned.size() - 1)) {
            int totalCount = partition.values().stream().mapToInt(count -> count).sum();
            Assert.assertEquals(partitionSize, totalCount);
        }

        assertTotalCountsInPartitionsMatchesOriginal(counts, partitioned);
        assertOrderingInPartitionsMatchesOriginal(counts, partitioned);
    }

    private void assertTotalCountsInPartitionsMatchesOriginal(Map<Integer, Integer> originalCounts,
                                                              List<Map<Integer, Integer>> partitioned) {
        Map<Integer, Integer> totalCountsAfterPartitioning = new HashMap<>();
        for (Map<Integer, Integer> partition : partitioned) {
            for (Map.Entry<Integer, Integer> entry : partition.entrySet()) {
                int prevCount = totalCountsAfterPartitioning.getOrDefault(entry.getKey(), 0);
                totalCountsAfterPartitioning.put(entry.getKey(), prevCount + entry.getValue());
            }
        }
        Assert.assertEquals(originalCounts, totalCountsAfterPartitioning);
    }

    private void assertOrderingInPartitionsMatchesOriginal(Map<Integer, Integer> counts,
                                                           List<Map<Integer, Integer>> partitioned) {
        List<Integer> originalOrder = ImmutableList.copyOf(counts.keySet());
        List<Integer> partitionedOrder = new ArrayList<>();
        for (Map<Integer, Integer> partition : partitioned) {
            for (int key : partition.keySet()) {
                if (!Objects.equals(Iterables.getLast(partitionedOrder, null), key)) {
                    partitionedOrder.add(key);
                }
            }
        }
        Assert.assertEquals(originalOrder, partitionedOrder);
    }
}
