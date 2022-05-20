/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.cassandra.AutoCloseableSupplier;
import com.palantir.atlasdb.futures.FuturesCombiner;
import com.palantir.atlasdb.keyvalue.api.AsyncKeyValueService;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.cassandra.async.queries.CqlQueryContext;
import com.palantir.atlasdb.keyvalue.cassandra.async.queries.GetQuerySpec;
import com.palantir.atlasdb.keyvalue.cassandra.async.queries.GetQuerySpec.GetQueryParameters;
import com.palantir.atlasdb.keyvalue.cassandra.async.queries.ImmutableCqlQueryContext;
import com.palantir.atlasdb.keyvalue.cassandra.async.queries.ImmutableGetQueryParameters;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Map;
import java.util.Optional;

public final class CassandraAsyncKeyValueService implements AsyncKeyValueService {
    private static final SafeLogger log = SafeLoggerFactory.get(CassandraAsyncKeyValueService.class);

    private final String keyspace;
    private final AutoCloseableSupplier<CqlClient> cqlClientContainer;
    private final FuturesCombiner futuresCombiner;

    public static AsyncKeyValueService create(
            String keyspace, AutoCloseableSupplier<CqlClient> cqlClientContainer, FuturesCombiner futuresCombiner) {
        return new CassandraAsyncKeyValueService(keyspace, cqlClientContainer, futuresCombiner);
    }

    private CassandraAsyncKeyValueService(
            String keyspace, AutoCloseableSupplier<CqlClient> cqlClientContainer, FuturesCombiner futuresCombiner) {
        this.keyspace = keyspace;
        this.cqlClientContainer = cqlClientContainer;
        this.futuresCombiner = futuresCombiner;
    }

    @Override
    public ListenableFuture<Map<Cell, Value>> getAsync(TableReference tableReference, Map<Cell, Long> timestampByCell) {
        if (log.isTraceEnabled()) {
            log.trace(
                    "Getting cells using CQL.",
                    SafeArg.of("cells", timestampByCell.size()),
                    LoggingArgs.tableRef(tableReference));
        }

        Map<Cell, ListenableFuture<Optional<Value>>> cellListenableFutureMap = KeyedStream.stream(timestampByCell)
                .map((cell, timestamp) -> getCellAsync(tableReference, cell, timestamp))
                .collectToMap();

        return futuresCombiner.allAsMap(cellListenableFutureMap);
    }

    private ListenableFuture<Optional<Value>> getCellAsync(TableReference tableReference, Cell cell, long timestamp) {
        CqlQueryContext queryContext = ImmutableCqlQueryContext.builder()
                .tableReference(tableReference)
                .keyspace(keyspace)
                .build();
        GetQueryParameters getQueryParameters = ImmutableGetQueryParameters.builder()
                .cell(cell)
                .humanReadableTimestamp(timestamp)
                .build();

        return cqlClientContainer.get().executeQuery(new GetQuerySpec(queryContext, getQueryParameters));
    }

    @Override
    public void close() {
        try {
            cqlClientContainer.close();
        } catch (Exception e) {
            log.warn("Failed to close the CQL Client Container", e);
        }
        futuresCombiner.close();
    }

    @Override
    public boolean isValid() {
        try {
            return !cqlClientContainer.isClosed() && cqlClientContainer.get().isValid();
        } catch (SafeIllegalStateException e) {
            // If cqlClientContainer is closed between the isClosed check and the get(), then the container will
            // throw on the latter call. The container isn't in a valid state, so we're not valid.
            return false;
        }
    }
}
