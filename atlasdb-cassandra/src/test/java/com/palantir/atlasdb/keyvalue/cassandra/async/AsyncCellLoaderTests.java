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

package com.palantir.atlasdb.keyvalue.cassandra.async;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;

public class AsyncCellLoaderTests {
    private static final String DEFAULT_KEYSPACE = "test";
    private static final TableReference DEFAULT_TABLE = TableReference.create(Namespace.DEFAULT_NAMESPACE, "foo");
    // tests are imagined as if the visible data has a timestamp lower than 20 and non visible data has timestamp higher
    private static final long TIMESTAMP = 20L;
    private static final Cell NON_VISIBLE_CELL = Cell.create(PtBytes.toBytes(100), PtBytes.toBytes(100));
    private static final Cell VISIBLE_CELL_1 = Cell.create(PtBytes.toBytes(100), PtBytes.toBytes(200));
    private static final Cell VISIBLE_CELL_2 = Cell.create(PtBytes.toBytes(100), PtBytes.toBytes(300));

    private CqlClient cqlClient;
    private final Executor testExecutor = MoreExecutors.directExecutor();

    @Before
    public void setUp() {
        cqlClient = mock(CqlClient.class);
    }

    @Test
    public void testNoDataVisible() throws Exception {
        when(cqlClient.executeQuery(any()))
                .thenReturn(Futures.immediateFuture(Optional.empty()));

        Map<Cell, Long> request = ImmutableMap.of(NON_VISIBLE_CELL, TIMESTAMP);
        AsyncCellLoader asyncCellLoader = AsyncCellLoader.create(cqlClient, testExecutor, DEFAULT_KEYSPACE);
        Map<Cell, Value> result = asyncCellLoader.loadAllWithTimestamp(DEFAULT_TABLE, request).get();
        assertThat(result.isEmpty()).isTrue();
    }

    @Test
    public void testFilteringNonVisible() throws Exception {
        when(cqlClient.executeQuery(any()))
                .thenReturn(Futures.immediateFuture(Optional.empty()))
                .thenReturn(Futures.immediateFuture(Optional.of(Value.create(PtBytes.toBytes("dummy"), 13))));

        Map<Cell, Long> request = ImmutableMap.of(NON_VISIBLE_CELL, TIMESTAMP, VISIBLE_CELL_1, TIMESTAMP);
        AsyncCellLoader asyncCellLoader = AsyncCellLoader.create(cqlClient, testExecutor, DEFAULT_KEYSPACE);
        Map<Cell, Value> result = asyncCellLoader.loadAllWithTimestamp(DEFAULT_TABLE, request).get();

        assertThat(result.isEmpty()).isFalse();
        assertThat(result.containsKey(VISIBLE_CELL_1)).isTrue();
        assertThat(result.containsKey(NON_VISIBLE_CELL)).isFalse();
    }

    @Test
    public void testAllVisible() throws Exception {
        when(cqlClient.executeQuery(any()))
                .thenReturn(Futures.immediateFuture(Optional.of(Value.create(PtBytes.toBytes("dummy"), 11))))
                .thenReturn(Futures.immediateFuture(Optional.of(Value.create(PtBytes.toBytes("dummy"), 13))));

        Map<Cell, Long> request = ImmutableMap.of(VISIBLE_CELL_1, TIMESTAMP, VISIBLE_CELL_2, TIMESTAMP);
        AsyncCellLoader asyncCellLoader = AsyncCellLoader.create(cqlClient, testExecutor, DEFAULT_KEYSPACE);
        Map<Cell, Value> result = asyncCellLoader.loadAllWithTimestamp(DEFAULT_TABLE, request).get();

        assertThat(result.isEmpty()).isFalse();
        assertThat(result.containsKey(VISIBLE_CELL_1)).isTrue();
        assertThat(result.containsKey(VISIBLE_CELL_2)).isTrue();
    }
}
