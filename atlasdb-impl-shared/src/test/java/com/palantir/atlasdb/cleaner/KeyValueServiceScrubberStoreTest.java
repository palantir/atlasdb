/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.cleaner;

import static org.assertj.core.api.Assertions.assertThat;

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
import java.util.List;
import java.util.SortedMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
        assertThat(getScrubQueue()).isEmpty();
    }

    @Test
    public void testSingleEntryStore() {
        Cell cell = Cell.create(new byte[] {1, 2, 3}, new byte[] {4, 5, 6});
        TableReference ref = TableReference.fromString("foo.bar");
        long timestamp = 10;
        scrubStore.queueCellsForScrubbing(ImmutableMultimap.of(cell, ref), timestamp, 1000);
        assertThat(getScrubQueue())
                .isEqualTo(ImmutableList.of(ImmutableSortedMap.of(timestamp, ImmutableMultimap.of(ref, cell))));
        scrubStore.markCellsAsScrubbed(ImmutableMap.of(ref, ImmutableMultimap.of(cell, timestamp)), 1000);
        assertThat(getScrubQueue()).isEmpty();
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
        assertThat(getScrubQueue())
                .isEqualTo(ImmutableList.of(ImmutableSortedMap.of(
                        timestamp1, ImmutableMultimap.of(ref1, cell2),
                        timestamp2, ImmutableMultimap.of(ref2, cell3, ref1, cell1))));
        scrubStore.markCellsAsScrubbed(
                ImmutableMap.of(
                        ref2, ImmutableMultimap.of(cell3, timestamp2),
                        ref1, ImmutableMultimap.of(cell1, timestamp1, cell1, timestamp2)),
                1000);
        assertThat(getScrubQueue())
                .isEqualTo(ImmutableList.of(ImmutableSortedMap.of(timestamp1, ImmutableMultimap.of(ref1, cell2))));
    }

    private List<SortedMap<Long, Multimap<TableReference, Cell>>> getScrubQueue() {
        BatchingVisitable<SortedMap<Long, Multimap<TableReference, Cell>>> visitable =
                scrubStore.getBatchingVisitableScrubQueue(
                        Long.MAX_VALUE, PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY);
        return BatchingVisitables.copyToList(visitable);
    }
}
