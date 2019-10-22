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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Optional;

import org.junit.After;
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
import com.palantir.atlasdb.keyvalue.cassandra.async.queries.CqlQueryContext;
import com.palantir.atlasdb.keyvalue.cassandra.async.queries.GetQuerySpec;
import com.palantir.atlasdb.keyvalue.cassandra.async.queries.GetQuerySpec.GetQueryParameters;
import com.palantir.atlasdb.keyvalue.cassandra.async.queries.ImmutableCqlQueryContext;
import com.palantir.atlasdb.keyvalue.cassandra.async.queries.ImmutableGetQueryParameters;
import com.palantir.atlasdb.keyvalue.cassandra.async.queries.ImmutableGetQuerySpec;

public class AsyncCellLoaderTests {
    private static final String KEYSPACE = "test";
    private static final TableReference TABLE = TableReference.create(Namespace.DEFAULT_NAMESPACE, "foo");
    // tests are imagined as if the visible data has a timestamp lower than 20 and non visible data has timestamp higher
    private static final long TIMESTAMP = 20L;
    private static final Cell NON_VISIBLE_CELL = Cell.create(PtBytes.toBytes(100), PtBytes.toBytes(100));
    private static final Cell VISIBLE_CELL_1 = Cell.create(PtBytes.toBytes(100), PtBytes.toBytes(200));
    private static final Cell VISIBLE_CELL_2 = Cell.create(PtBytes.toBytes(100), PtBytes.toBytes(300));
    private static final CqlQueryContext cqlQueryContext = ImmutableCqlQueryContext.builder()
            .keyspace(KEYSPACE)
            .tableReference(TABLE)
            .build();

    private AsyncCellLoader asyncCellLoader;

    @Before
    public void setUp() {
        CqlClient cqlClient = mock(CqlClient.class);

        when(cqlClient.executeQuery(buildGetQuerySpec(buildGetQueryParameter(NON_VISIBLE_CELL))))
                .thenReturn(Futures.immediateFuture(Optional.empty()));
        when(cqlClient.executeQuery(buildGetQuerySpec(buildGetQueryParameter(VISIBLE_CELL_1))))
                .thenReturn(Futures.immediateFuture(Optional.of(Value.create(PtBytes.toBytes("dummy"), 13))));
        when(cqlClient.executeQuery(buildGetQuerySpec(buildGetQueryParameter(VISIBLE_CELL_2))))
                .thenReturn(Futures.immediateFuture(Optional.of(Value.create(PtBytes.toBytes("dummy"), 11))));

        asyncCellLoader = AsyncCellLoader.create(cqlClient, MoreExecutors.newDirectExecutorService(), KEYSPACE);
    }

    @After
    public void tearDown() {
        asyncCellLoader.close();
    }

    @Test
    public void testNoDataVisible() throws Exception {
        Map<Cell, Long> request = ImmutableMap.of(NON_VISIBLE_CELL, TIMESTAMP);

        Map<Cell, Value> result = asyncCellLoader.loadAllWithTimestamp(TABLE, request).get();

        assertThat(result).isEmpty();
    }

    @Test
    public void testFilteringNonVisible() throws Exception {
        Map<Cell, Long> request = ImmutableMap.of(
                NON_VISIBLE_CELL, TIMESTAMP,
                VISIBLE_CELL_1, TIMESTAMP);

        Map<Cell, Value> result = asyncCellLoader.loadAllWithTimestamp(TABLE, request).get();

        assertThat(result)
                .isNotEmpty()
                .containsKey(VISIBLE_CELL_1)
                .doesNotContainKey(NON_VISIBLE_CELL);
    }

    @Test
    public void testAllVisible() throws Exception {
        Map<Cell, Long> request = ImmutableMap.of(
                VISIBLE_CELL_1, TIMESTAMP,
                VISIBLE_CELL_2, TIMESTAMP);

        Map<Cell, Value> result = asyncCellLoader.loadAllWithTimestamp(TABLE, request).get();

        assertThat(result)
                .isNotEmpty()
                .containsKey(VISIBLE_CELL_1)
                .containsKey(VISIBLE_CELL_2);
    }

    private GetQuerySpec buildGetQuerySpec(GetQueryParameters getQueryParameters) {
        return ImmutableGetQuerySpec
                .builder()
                .cqlQueryContext(cqlQueryContext)
                .queryParameters(getQueryParameters)
                .build();
    }

    private GetQueryParameters buildGetQueryParameter(Cell cell) {
        return ImmutableGetQueryParameters.builder()
                .cell(cell)
                .humanReadableTimestamp(TIMESTAMP)
                .build();
    }
}
