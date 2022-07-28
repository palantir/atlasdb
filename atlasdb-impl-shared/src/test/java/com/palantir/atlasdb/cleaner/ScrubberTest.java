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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.transaction.impl.TransactionTables;
import com.palantir.atlasdb.transaction.service.SimpleTransactionService;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.common.base.BatchingVisitables;
import java.util.Collection;
import java.util.List;
import java.util.SortedMap;
import java.util.function.Function;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class ScrubberTest {
    private KeyValueService kvs;
    private TransactionService transactions;
    private ScrubberStore scrubStore;
    private Scrubber scrubber;

    @Parameterized.Parameter
    public Function<KeyValueService, TransactionService> transactionServiceForKvs;

    @Parameterized.Parameters
    public static Collection<Function<KeyValueService, TransactionService>> parameters() {
        return ImmutableList.of(SimpleTransactionService::createV1, SimpleTransactionService::createV2);
    }

    @Before
    public void before() {
        kvs = new InMemoryKeyValueService(false, MoreExecutors.newDirectExecutorService());
        TransactionTables.createTables(kvs);
        transactions = transactionServiceForKvs.apply(kvs);
        scrubStore = KeyValueServiceScrubberStore.create(kvs);
        scrubber = getScrubber(kvs, scrubStore, transactions);
    }

    @After
    public void after() {
        scrubber.shutdown();
        kvs.close();
    }

    @Test
    public void isInitializedWhenPrerequisitesAreInitialized() {
        KeyValueService mockKvs = mock(KeyValueService.class);
        ScrubberStore mockStore = mock(ScrubberStore.class);
        when(mockKvs.isInitialized()).thenReturn(true);
        when(mockStore.isInitialized()).thenReturn(true);

        Scrubber theScrubber = getScrubber(mockKvs, mockStore, transactions);

        assertThat(theScrubber.isInitialized()).isTrue();
    }

    @Test
    public void isNotInitializedWhenKvsIsNotInitialized() {
        KeyValueService mockKvs = mock(KeyValueService.class);
        ScrubberStore mockStore = mock(ScrubberStore.class);
        when(mockKvs.isInitialized()).thenReturn(false);
        when(mockStore.isInitialized()).thenReturn(true);

        Scrubber theScrubber = getScrubber(mockKvs, mockStore, transactions);

        assertThat(theScrubber.isInitialized()).isFalse();
    }

    @Test
    public void isNotInitializedWhenScrubberStoreIsNotInitialized() {
        KeyValueService mockKvs = mock(KeyValueService.class);
        ScrubberStore mockStore = mock(ScrubberStore.class);
        when(mockKvs.isInitialized()).thenReturn(true);
        when(mockStore.isInitialized()).thenReturn(false);

        Scrubber theScrubber = getScrubber(mockKvs, mockStore, transactions);

        assertThat(theScrubber.isInitialized()).isFalse();
    }

    @Test
    public void testScrubQueueIsCleared() {
        Cell cell1 = Cell.create(new byte[] {1}, new byte[] {2});
        Cell cell2 = Cell.create(new byte[] {2}, new byte[] {3});
        Cell cell3 = Cell.create(new byte[] {3}, new byte[] {4});
        TableReference tableRef = TableReference.createFromFullyQualifiedName("foo.bar");
        kvs.createTable(tableRef, new byte[] {});
        kvs.putWithTimestamps(
                tableRef,
                ImmutableMultimap.<Cell, Value>builder()
                        .put(cell1, Value.create(new byte[] {3}, 10))
                        .put(cell1, Value.create(new byte[] {4}, 20))
                        .put(cell2, Value.create(new byte[] {4}, 30))
                        .put(cell2, Value.create(new byte[] {5}, 40))
                        .put(cell2, Value.create(new byte[] {6}, 50))
                        .put(cell3, Value.create(new byte[] {7}, 60))
                        .build());
        transactions.putUnlessExists(10, 15);
        transactions.putUnlessExists(20, 25);
        transactions.putUnlessExists(30, 35);
        transactions.putUnlessExists(50, 55);
        transactions.putUnlessExists(60, 65);
        scrubStore.queueCellsForScrubbing(ImmutableMultimap.of(cell1, tableRef), 10, 100);
        scrubStore.queueCellsForScrubbing(ImmutableMultimap.of(cell1, tableRef), 20, 100);
        scrubStore.queueCellsForScrubbing(ImmutableMultimap.of(cell2, tableRef), 40, 100);
        scrubStore.queueCellsForScrubbing(ImmutableMultimap.of(cell2, tableRef), 50, 100);
        scrubStore.queueCellsForScrubbing(ImmutableMultimap.of(cell3, tableRef), 60, 100);
        scrubber.runBackgroundScrubTask(null);

        List<SortedMap<Long, Multimap<TableReference, Cell>>> scrubQueue =
                BatchingVisitables.copyToList(scrubStore.getBatchingVisitableScrubQueue(Long.MAX_VALUE, null, null));
        assertThat(scrubQueue).isEmpty();
    }

    private Scrubber getScrubber(
            KeyValueService keyValueService, ScrubberStore scrubberStore, TransactionService transactionService) {
        return Scrubber.create(
                keyValueService,
                scrubberStore,
                () -> Long.MAX_VALUE, // background scrub frequency millis
                () -> true, // scrub enabled
                () -> 100L, // unreadable timestamp
                () -> 100L, // immutable timestamp
                transactionService,
                false, // is aggressive
                () -> 100, //  batch size
                1, // thread count
                1, // read thread count
                ImmutableList.of(), // followers
                MetricsManagers.createForTests());
    }
}
