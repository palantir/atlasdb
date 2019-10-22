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

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.cassandra.async.queries.CqlQueryContext;
import com.palantir.atlasdb.keyvalue.cassandra.async.queries.GetQuerySpec.GetQueryParameters;
import com.palantir.atlasdb.keyvalue.cassandra.async.queries.ImmutableCqlQueryContext;
import com.palantir.atlasdb.keyvalue.cassandra.async.queries.ImmutableGetQueryParameters;
import com.palantir.atlasdb.keyvalue.cassandra.async.queries.ImmutableGetQuerySpec;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.SafeArg;

public final class AsyncCellLoader implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(AsyncCellLoader.class);

    private final CqlClient cqlClient;
    private final ExecutorService executorService;
    private final String keyspace;

    public static AsyncCellLoader create(CqlClient cqlClient, ExecutorService executorService, String keyspace) {
        return new AsyncCellLoader(cqlClient, executorService, keyspace);
    }

    private AsyncCellLoader(CqlClient cqlClient, ExecutorService executorService, String keyspace) {
        this.cqlClient = cqlClient;
        this.executorService = executorService;
        this.keyspace = keyspace;
    }

    public ListenableFuture<Map<Cell, Value>> loadAllWithTimestamp(
            TableReference tableRef,
            Map<Cell, Long> timestampByCell) {
        if (log.isTraceEnabled()) {
            log.trace(
                    "Loading cells using CQL.",
                    SafeArg.of("cells", timestampByCell.size()),
                    LoggingArgs.tableRef(tableRef));
        }

        Map<Cell, ListenableFuture<Optional<Value>>> cellListenableFutureMap = KeyedStream.stream(timestampByCell)
                .map((cell, timestamp) -> loadCellWithTimestamp(tableRef, cell, timestamp))
                .collectToMap();

        return Futures.whenAllSucceed(cellListenableFutureMap.values())
                .call(() -> KeyedStream.stream(cellListenableFutureMap)
                                .map(AsyncCellLoader::getDone)
                                .filter(Optional::isPresent)
                                .map(Optional::get)
                                .collectToMap(),
                        executorService);
    }

    private ListenableFuture<Optional<Value>> loadCellWithTimestamp(
            TableReference tableRef,
            Cell cell,
            long timestamp) {
        CqlQueryContext queryContext = ImmutableCqlQueryContext.builder()
                .tableReference(tableRef)
                .keyspace(keyspace)
                .build();
        GetQueryParameters getQueryParameters = ImmutableGetQueryParameters.builder()
                .cell(cell)
                .humanReadableTimestamp(timestamp)
                .build();

        return cqlClient.executeQuery(
                ImmutableGetQuerySpec.builder()
                        .cqlQueryContext(queryContext)
                        .queryParameters(getQueryParameters)
                        .build());
    }

    private static <V> V getDone(Future<V> listenableFuture) {
        try {
            return Futures.getDone(listenableFuture);
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }
    }

    @Override
    public void close() {
        executorService.shutdown();
    }
}
