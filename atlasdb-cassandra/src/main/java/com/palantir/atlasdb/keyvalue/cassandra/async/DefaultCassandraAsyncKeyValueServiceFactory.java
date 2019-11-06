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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.InstrumentedExecutorService;
import com.codahale.metrics.MetricRegistry;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.api.AsyncKeyValueService;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueService;
import com.palantir.atlasdb.keyvalue.cassandra.async.client.creation.CqlClientFactory;
import com.palantir.atlasdb.keyvalue.cassandra.async.client.creation.CqlClientFactoryImpl;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.tracing.Tracers;

public class DefaultCassandraAsyncKeyValueServiceFactory implements CassandraAsyncKeyValueServiceFactory {
    public static final CassandraAsyncKeyValueServiceFactory DEFAULT =
            new DefaultCassandraAsyncKeyValueServiceFactory(CqlClientFactoryImpl.DEFAULT);

    private final CqlClientFactory cqlClientFactory;

    private DefaultCassandraAsyncKeyValueServiceFactory(CqlClientFactory cqlClientFactory) {
        this.cqlClientFactory = cqlClientFactory;
    }

    @Override
    public AsyncKeyValueService constructAsyncKeyValueService(
            MetricsManager metricsManager,
            CassandraKeyValueServiceConfig config,
            boolean initializeAsync) {
        CqlClient cqlClient = cqlClientFactory.constructClient(
                metricsManager.getTaggedRegistry(),
                config,
                initializeAsync);

        return DefaultCassandraAsyncKeyValueService.create(config.getKeyspaceOrThrow(), cqlClient, Tracers.wrap(
                new InstrumentedExecutorService(
                        createThreadPool(
                                config.servers().numberOfThriftHosts() * config.poolSize()),
                        metricsManager.getRegistry(),
                        MetricRegistry.name(CassandraKeyValueService.class, "executorService"))));
    }

    /**
     * Creates a thread pool with number of threads between 0 and {@code maxPoolSize}.
     *
     * @param maxPoolSize      maximum size of the pool
     * @return a new fixed size thread pool with a keep alive time of 1 minute
     */
    private static ExecutorService createThreadPool(int maxPoolSize) {
        return PTExecutors.newThreadPoolExecutor(0, maxPoolSize,
                1, TimeUnit.MINUTES,
                new LinkedBlockingQueue<>(), new NamedThreadFactory("Atlas Cassandra Async KVS", false));
    }
}
