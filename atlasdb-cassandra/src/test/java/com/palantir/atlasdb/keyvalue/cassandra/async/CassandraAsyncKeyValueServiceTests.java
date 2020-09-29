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
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.keyvalue.api.AsyncKeyValueService;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.cassandra.async.queries.CqlQueryContext;
import com.palantir.atlasdb.keyvalue.cassandra.async.queries.GetQuerySpec;
import com.palantir.atlasdb.keyvalue.cassandra.async.queries.ImmutableCqlQueryContext;
import com.palantir.atlasdb.keyvalue.cassandra.async.queries.ImmutableGetQueryParameters;
import com.palantir.common.random.RandomBytes;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CassandraAsyncKeyValueServiceTests {
    private static final String KEYSPACE = "test";
    private static final TableReference TABLE = TableReference.create(Namespace.DEFAULT_NAMESPACE, "foo");
    // tests are imagined as if the visible data has a timestamp lower than 20 and non visible data has timestamp higher
    private static final long TIMESTAMP = 20L;
    private static final Cell NON_VISIBLE_CELL = Cell.create(PtBytes.toBytes(100), PtBytes.toBytes(100));
    private static final Cell VISIBLE_CELL_1 = Cell.create(PtBytes.toBytes(100), PtBytes.toBytes(200));
    private static final Cell VISIBLE_CELL_2 = Cell.create(PtBytes.toBytes(100), PtBytes.toBytes(300));
    private static final CqlQueryContext CQL_QUERY_CONTEXT = ImmutableCqlQueryContext.builder()
            .keyspace(KEYSPACE)
            .tableReference(TABLE)
            .build();

    private AsyncKeyValueService asyncKeyValueService;
    @Mock
    private CqlClient cqlClient;

    @Before
    public void setUp() {
        asyncKeyValueService = CassandraAsyncKeyValueService.create(
                KEYSPACE,
                cqlClient,
                AtlasFutures.futuresCombiner(MoreExecutors.newDirectExecutorService()));
    }

    @After
    public void tearDown() {
        asyncKeyValueService.close();
    }

    @Test
    public void testNoDataVisible() throws Exception {
        setUpNonVisibleCells(NON_VISIBLE_CELL);

        Map<Cell, Long> request = ImmutableMap.of(NON_VISIBLE_CELL, TIMESTAMP);
        Map<Cell, Value> result = asyncKeyValueService.getAsync(TABLE, request).get();

        assertThat(result).isEmpty();
    }

    @Test
    public void testFilteringNonVisible() throws Exception {
        setUpVisibleCells(VISIBLE_CELL_1);
        setUpNonVisibleCells(NON_VISIBLE_CELL);

        Map<Cell, Long> request = ImmutableMap.of(
                NON_VISIBLE_CELL, TIMESTAMP,
                VISIBLE_CELL_1, TIMESTAMP);
        Map<Cell, Value> result = asyncKeyValueService.getAsync(TABLE, request).get();

        assertThat(result).containsOnlyKeys(VISIBLE_CELL_1);
    }

    @Test
    public void testAllVisible() throws Exception {
        setUpVisibleCells(VISIBLE_CELL_1, VISIBLE_CELL_2);

        Map<Cell, Long> request = ImmutableMap.of(
                VISIBLE_CELL_1, TIMESTAMP,
                VISIBLE_CELL_2, TIMESTAMP);
        Map<Cell, Value> result = asyncKeyValueService.getAsync(TABLE, request).get();

        assertThat(result).containsOnlyKeys(VISIBLE_CELL_1, VISIBLE_CELL_2);
    }

    private void setUpVisibleCells(Cell... cells) {
        for (Cell cell : cells) {
            when(cqlClient.executeQuery(buildGetQuerySpec(buildGetQueryParameter(cell))))
                    .thenReturn(Futures.immediateFuture(
                            Optional.of(Value.create(RandomBytes.ofLength(10), Math.abs(new Random().nextInt())))));
        }
    }

    private void setUpNonVisibleCells(Cell... nonVisibleCells) {
        for (Cell cell : nonVisibleCells) {
            when(cqlClient.executeQuery(buildGetQuerySpec(buildGetQueryParameter(cell))))
                    .thenReturn(Futures.immediateFuture(Optional.empty()));
        }
    }

    private static GetQuerySpec buildGetQuerySpec(GetQuerySpec.GetQueryParameters getQueryParameters) {
        return new GetQuerySpec(CQL_QUERY_CONTEXT, getQueryParameters);
    }

    private static GetQuerySpec.GetQueryParameters buildGetQueryParameter(Cell cell) {
        return ImmutableGetQueryParameters.builder()
                .cell(cell)
                .humanReadableTimestamp(TIMESTAMP)
                .build();
    }
}
