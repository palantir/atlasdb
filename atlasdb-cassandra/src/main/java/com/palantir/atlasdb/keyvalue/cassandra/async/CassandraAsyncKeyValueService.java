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

import com.google.common.util.concurrent.ListenableFuture;
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
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Map;
import java.util.Optional;

public final class CassandraAsyncKeyValueService implements AsyncKeyValueService {
    private static final SafeLogger log = SafeLoggerFactory.get(CassandraAsyncKeyValueService.class);

    private final String keyspace;
    private final CqlClient cqlClient;
    private final FuturesCombiner futuresCombiner;

    public static AsyncKeyValueService create(String keyspace, CqlClient cqlClient, FuturesCombiner futuresCombiner) {
        return new CassandraAsyncKeyValueService(keyspace, cqlClient, futuresCombiner);
    }

    private CassandraAsyncKeyValueService(String keyspace, CqlClient cqlClient, FuturesCombiner futuresCombiner) {
        this.keyspace = keyspace;
        this.cqlClient = cqlClient;
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

        return cqlClient.executeQuery(new GetQuerySpec(queryContext, getQueryParameters));
    }

    @Override
    public void close() {
        cqlClient.close();
        futuresCombiner.close();
    }
}
