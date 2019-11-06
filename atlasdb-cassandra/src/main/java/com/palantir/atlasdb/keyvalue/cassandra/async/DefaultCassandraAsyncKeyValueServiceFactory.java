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
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.keyvalue.api.AsyncKeyValueService;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.cassandra.async.client.creation.CqlClientFactory;
import com.palantir.atlasdb.keyvalue.cassandra.async.queries.CqlQueryContext;
import com.palantir.atlasdb.keyvalue.cassandra.async.queries.GetQuerySpec;
import com.palantir.atlasdb.keyvalue.cassandra.async.queries.ImmutableCqlQueryContext;
import com.palantir.atlasdb.keyvalue.cassandra.async.queries.ImmutableGetQueryParameters;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.SafeArg;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

public class DefaultCassandraAsyncKeyValueServiceFactory implements CassandraAsyncKeyValueServiceFactory {
    private final CqlClientFactory cqlClientFactory;

    public DefaultCassandraAsyncKeyValueServiceFactory(CqlClientFactory cqlClientFactory) {
        this.cqlClientFactory = cqlClientFactory;
    }

    @Override
    public AsyncKeyValueService constructAsyncKeyValueService(
            TaggedMetricRegistry taggedMetricRegistry,
            CassandraKeyValueServiceConfig config,
            boolean initializeAsync) {
        CqlClient cqlClient = cqlClientFactory.constructClient(taggedMetricRegistry, config, initializeAsync);

        return null;
    }

    private static final class DefaultAsyncKeyValueService implements AsyncKeyValueService {
        private static final Logger log = LoggerFactory.getLogger(DefaultAsyncKeyValueService.class);

        private final CqlClient cqlClient;
        private final ExecutorService executorService;
        private final String keyspace;

        public static DefaultAsyncKeyValueService create(
                String keyspace,
                CqlClient cqlClient,
                ExecutorService executorService) {
            return new DefaultAsyncKeyValueService(keyspace, cqlClient, executorService);
        }

        private DefaultAsyncKeyValueService(String keyspace, CqlClient cqlClient, ExecutorService executorService) {
            this.keyspace = keyspace;
            this.cqlClient = cqlClient;
            this.executorService = executorService;
        }

        public ListenableFuture<Map<Cell, Value>> getAsync(
                TableReference tableReference,
                Map<Cell, Long> timestampByCell) {
            if (log.isTraceEnabled()) {
                log.trace(
                        "Loading cells using CQL.",
                        SafeArg.of("cells", timestampByCell.size()),
                        LoggingArgs.tableRef(tableReference));
            }

            Map<Cell, ListenableFuture<Optional<Value>>> cellListenableFutureMap = KeyedStream.stream(timestampByCell)
                    .map((cell, timestamp) -> getCellAsync(tableReference, cell, timestamp))
                    .collectToMap();

            return AtlasFutures.allAsMap(cellListenableFutureMap, executorService);
        }

        private ListenableFuture<Optional<Value>> getCellAsync(
                TableReference tableReference,
                Cell cell,
                long timestamp) {
            CqlQueryContext queryContext = ImmutableCqlQueryContext.builder()
                    .tableReference(tableReference)
                    .keyspace(keyspace)
                    .build();
            GetQuerySpec.GetQueryParameters getQueryParameters = ImmutableGetQueryParameters.builder()
                    .cell(cell)
                    .humanReadableTimestamp(timestamp)
                    .build();

            return cqlClient.executeQuery(new GetQuerySpec(queryContext, getQueryParameters));
        }

        @Override
        public void close() {
            executorService.shutdown();
        }
    }
}
