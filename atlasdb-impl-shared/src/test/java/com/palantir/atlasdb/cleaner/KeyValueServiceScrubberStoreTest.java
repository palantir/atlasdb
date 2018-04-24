/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
 */

package com.palantir.atlasdb.cleaner;

import java.util.List;
import java.util.SortedMap;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.common.base.BatchingVisitable;
import com.palantir.common.base.BatchingVisitables;

public class KeyValueServiceScrubberStoreTest {
    private KeyValueService kvs;
    private ScrubberStore scrubStore;

    @Before
    public void before() {
        kvs = new InMemoryKeyValueService(false, MoreExecutors.newDirectExecutorService());
        scrubStore = KeyValueServiceScrubberStore.create(kvs);
    }

    @After
    public void after() {
        kvs.close();
    }

    @Test
    public void testEmptyStoreReturnsNothing() {
        Assert.assertEquals(ImmutableList.of(), getScrubQueue());
    }

    @Test
    public void testSingleEntryStore() {
        Cell cell = Cell.create(new byte[] {1, 2, 3}, new byte[] {4, 5, 6});
        TableReference ref = TableReference.fromString("foo.bar");
        long timestamp = 10;
        scrubStore.queueCellsForScrubbing(ImmutableMultimap.of(cell, ref), timestamp, 1000);
        Assert.assertEquals(
                ImmutableList.of(ImmutableSortedMap.of(timestamp, ImmutableMultimap.of(ref, cell))),
                getScrubQueue());
        scrubStore.markCellsAsScrubbed(ImmutableMap.of(ref, ImmutableMultimap.of(cell, timestamp)), 1000);
        Assert.assertEquals(ImmutableList.of(), getScrubQueue());
    }

    @Test
    public void testMultipleEntryStore() {
        Cell cell1 = Cell.create(new byte[] {1, 2, 3}, new byte[] {4, 5, 6});
        Cell cell2 = Cell.create(new byte[] {7, 8, 9}, new byte[] {4, 5, 6});
        Cell cell3 = Cell.create(new byte[] {1, 2, 3}, new byte[] {7, 8, 9});
        TableReference ref1 = TableReference.fromString("foo.bar");
        TableReference ref2 = TableReference.fromString("foo.baz");
        long timestamp1 = 10;
        long timestamp2 = 20;
        scrubStore.queueCellsForScrubbing(ImmutableMultimap.of(cell1, ref1, cell2, ref1), timestamp1, 1000);
        scrubStore.queueCellsForScrubbing(ImmutableMultimap.of(cell1, ref1, cell3, ref2), timestamp2, 1000);
        Assert.assertEquals(
                ImmutableList.of(ImmutableSortedMap.of(
                        timestamp1, ImmutableMultimap.of(ref1, cell2),
                        timestamp2, ImmutableMultimap.of(ref2, cell3, ref1, cell1))),
                getScrubQueue());
        scrubStore.markCellsAsScrubbed(ImmutableMap.of(
                ref2, ImmutableMultimap.of(cell3, timestamp2),
                ref1, ImmutableMultimap.of(cell1, timestamp1, cell1, timestamp2)), 1000);
        Assert.assertEquals(
                ImmutableList.of(ImmutableSortedMap.of(
                        timestamp1, ImmutableMultimap.of(ref1, cell2))),
                getScrubQueue());
    }

    private List<SortedMap<Long, Multimap<TableReference, Cell>>> getScrubQueue() {
        BatchingVisitable<SortedMap<Long, Multimap<TableReference, Cell>>> visitable =
                scrubStore.getBatchingVisitableScrubQueue(
                        Long.MAX_VALUE, PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY);
        return BatchingVisitables.copyToList(visitable);
    }
}
