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

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.SafeArg;

public final class AsyncCellLoader {

    private static final Logger log = LoggerFactory.getLogger(AsyncCellLoader.class);

    private final CqlClient cqlClient;
    private final Executor executor;
    private final String keyspace;

    public static AsyncCellLoader create(CqlClient cqlClient, Executor executor, String keyspace) {
        return new AsyncCellLoader(cqlClient, executor, keyspace);
    }

    private AsyncCellLoader(CqlClient cqlClient, Executor executor, String keyspace) {
        this.cqlClient = cqlClient;
        this.executor = executor;
        this.keyspace = keyspace;
    }

    public ListenableFuture<Map<Cell, Value>> loadAllWithTimestamp(
            TableReference tableRef,
            Set<Cell> cells,
            long startTimestamp) {
        if (log.isTraceEnabled()) {
            log.trace(
                    "Loading {} cells from {} starting at timestamp {}, using CQL.",
                    SafeArg.of("cells", cells.size()),
                    LoggingArgs.tableRef(tableRef),
                    SafeArg.of("startTs", startTimestamp));
        }

        Map<Cell, ListenableFuture<Optional<Value>>> cellsToFutureResults = cells.stream()
                .map(cell -> Maps.immutableEntry(cell, loadCellWithTimestamp(tableRef, cell, startTimestamp)))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        return Futures.whenAllSucceed(cellsToFutureResults.values())
                .call(() -> KeyedStream.stream(cellsToFutureResults)
                                .map(AsyncCellLoader::getDone)
                                .filter(Optional::isPresent)
                                .map(Optional::get)
                                .collectToMap(),
                        executor);
    }

    private ListenableFuture<Optional<Value>> loadCellWithTimestamp(
            TableReference tableRef,
            Cell cell,
            long timestamp) {
        return cqlClient.executeQuery(
                ImmutableGetQuerySpec.builder()
                        .tableReference(tableRef)
                        .keySpace(keyspace)
                        .row(ByteBuffer.wrap(cell.getRowName()))
                        .column(ByteBuffer.wrap(cell.getColumnName()))
                        .humanReadableTimestamp(timestamp)
                        .executor(executor)
                        .queryConsistency(com.datastax.driver.core.ConsistencyLevel.LOCAL_QUORUM)
                        .build());
    }

    public static  <V> V getDone(Future<V> listenableFuture) {
        try {
            return Futures.getDone(listenableFuture);
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }
    }
}
