/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.impl.illiteracy;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.timelock.watch.WatchIndexState;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.transaction.service.TransactionServices;

public class RowStateCacheTest {
    private static final TableReference TEST_TABLE = TableReference.createFromFullyQualifiedName("a.b");

    private static final byte[] BYTES = { 1, 2, 3 };
    private static final RowReference ROW_REFERENCE = ImmutableRowReference.builder()
            .tableReference(TEST_TABLE)
            .row(BYTES)
            .build();

    private final KeyValueService kvs = new InMemoryKeyValueService(true);
    private final TransactionService transactionService = TransactionServices.createV1TransactionService(kvs);
    private final RowStateCache cache = new RowStateCache(kvs, transactionService);

    @Before
    public void makeTestTable() {
        kvs.createTable(TEST_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);
        kvs.truncateTable(TEST_TABLE);
    }

    @Test
    public void updates() throws ExecutionException, InterruptedException {
        Optional<Map<Cell, Value>> cacheValue = cache.get(ROW_REFERENCE, WatchIndexState.of(1, 2), 3);
        assertThat(cacheValue).isEmpty();

        // Flush the cache
        cache.updater.apply(ImmutableRowCacheUpdateRequest.builder()
                .rowReference(ROW_REFERENCE)
                .readTimestamp(66)
                .watchIndexState(WatchIndexState.of(3, 4))
                .build()).get();
        assertThat(cache.backingMap).hasSize(1);
        assertThat(cache.backingMap.get(ROW_REFERENCE)).satisfies(rscv -> {
            assertThat(rscv.data()).isEmpty();
            assertThat(rscv.earliestValidTimestamp()).isEqualTo(66);
            assertThat(rscv.watchIndexState()).isEqualTo(WatchIndexState.of(3, 4));
        });
    }

    @Test
    public void cachesStuff() throws ExecutionException, InterruptedException {
        kvs.put(TEST_TABLE, ImmutableMap.of(Cell.create(BYTES, PtBytes.toBytes("a")), PtBytes.toBytes("b")), 10);
        transactionService.putUnlessExists(10, 45);

        // Nothing was cached!
        Optional<Map<Cell, Value>> cacheValue = cache.get(ROW_REFERENCE, WatchIndexState.of(1, 2), 3);
        assertThat(cacheValue).isEmpty();

        // Flush the cache
        cache.updater.apply(ImmutableRowCacheUpdateRequest.builder()
                .rowReference(ROW_REFERENCE)
                .readTimestamp(44)
                .watchIndexState(WatchIndexState.of(3, 4))
                .build()).get();
        assertThat(cache.backingMap).hasSize(1);
        assertThat(cache.backingMap.get(ROW_REFERENCE)).satisfies(rscv -> {
            assertThat(rscv.data()).containsExactly(
                    Maps.immutableEntry(Cell.create(BYTES, PtBytes.toBytes("a")),
                            Value.create(PtBytes.toBytes("b"), 10)));
            assertThat(rscv.earliestValidTimestamp()).isEqualTo(45);
            assertThat(rscv.watchIndexState()).isEqualTo(WatchIndexState.of(3, 4));
        });

        // OK to read cache
        cacheValue = cache.get(ROW_REFERENCE, WatchIndexState.of(3, 4), 71);
        assertThat(cacheValue).contains(
                ImmutableMap.of(Cell.create(BYTES, PtBytes.toBytes("a")), Value.create(PtBytes.toBytes("b"), 10))
        );

        // Cannot return cached value because it predates us
        cacheValue = cache.get(ROW_REFERENCE, WatchIndexState.of(3, 4), 35);
        assertThat(cacheValue).isEmpty();

        // Cannot return cached value because the locks have changed
        cacheValue = cache.get(ROW_REFERENCE, WatchIndexState.of(1005, 1006), 352);
        assertThat(cacheValue).isEmpty();
    }
}
