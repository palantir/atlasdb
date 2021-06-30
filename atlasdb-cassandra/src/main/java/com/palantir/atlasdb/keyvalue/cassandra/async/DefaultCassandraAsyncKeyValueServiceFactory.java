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

import com.codahale.metrics.InstrumentedExecutorService;
import com.codahale.metrics.MetricRegistry;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs.CqlCapableConfig;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs.DefaultConfig;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs.Visitor;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.keyvalue.api.AsyncKeyValueService;
import com.palantir.atlasdb.keyvalue.cassandra.async.client.creation.CqlClientFactory;
import com.palantir.atlasdb.keyvalue.cassandra.async.client.creation.DefaultCqlClientFactory;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.tracing.Tracers;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

public final class DefaultCassandraAsyncKeyValueServiceFactory implements CassandraAsyncKeyValueServiceFactory {
    public static final CassandraAsyncKeyValueServiceFactory DEFAULT =
            new DefaultCassandraAsyncKeyValueServiceFactory(DefaultCqlClientFactory.DEFAULT);

    private final CqlClientFactory cqlClientFactory;

    public DefaultCassandraAsyncKeyValueServiceFactory(CqlClientFactory cqlClientFactory) {
        this.cqlClientFactory = cqlClientFactory;
    }

    @Override
    public Optional<AsyncKeyValueService> constructAsyncKeyValueService(
            MetricsManager metricsManager, CassandraKeyValueServiceConfig config, boolean initializeAsync) {
        Optional<CqlClient> cqlClient =
                cqlClientFactory.constructClient(metricsManager.getTaggedRegistry(), config, initializeAsync);

        ExecutorService executorService = config.servers().accept(new Visitor<ExecutorService>() {
            @Override
            public ExecutorService visit(DefaultConfig defaultConfig) {
                return MoreExecutors.newDirectExecutorService();
            }

            @Override
            public ExecutorService visit(CqlCapableConfig cqlCapableConfig) {
                if (cqlCapableConfig.cqlHosts().isEmpty()) {
                    return MoreExecutors.newDirectExecutorService();
                }
                return tracingExecutorService(
                        "Atlas Cassandra Async KVS",
                        instrumentExecutorService(
                                createThreadPool(cqlCapableConfig.cqlHosts().size() * config.poolSize()),
                                metricsManager));
            }
        });

        return cqlClient.map(client -> CassandraAsyncKeyValueService.create(
                config.getKeyspaceOrThrow(), client, AtlasFutures.futuresCombiner(executorService)));
    }

    /**
     * Creates a thread pool with number of threads between 0 and {@code maxPoolSize}.
     *
     * @param maxPoolSize      maximum size of the pool
     * @return a new dynamic thread pool with a thread keep alive time of 1 minute
     */
    private static ExecutorService createThreadPool(int maxPoolSize) {
        return PTExecutors.newFixedThreadPool(maxPoolSize, "Atlas Cassandra Async KVS");
    }

    private ExecutorService tracingExecutorService(String operationName, ExecutorService executorService) {
        return Tracers.wrap(operationName, executorService);
    }

    private ExecutorService instrumentExecutorService(ExecutorService executorService, MetricsManager metricsManager) {
        return new InstrumentedExecutorService(
                executorService,
                metricsManager.getRegistry(),
                MetricRegistry.name(AsyncKeyValueService.class, "cassandra.executorService"));
    }
}
