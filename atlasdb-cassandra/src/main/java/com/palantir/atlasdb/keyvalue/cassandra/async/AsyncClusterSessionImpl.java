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
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Metric;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.keyvalue.cassandra.async.AsyncSessionManager.CassandraClusterSessionPair;
import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.SafeArg;
import com.palantir.tritium.metrics.registry.MetricName;

public final class AsyncClusterSessionImpl implements AsyncClusterSession {

    private static final Logger log = LoggerFactory.getLogger(AsyncClusterSessionImpl.class);

    private final CassandraClusterSessionPair pair;
    private final String sessionName;
    private final Executor executor;

    private final ScheduledExecutorService service = PTExecutors.newSingleThreadScheduledExecutor(
            new NamedThreadFactory(sessionName() + "-healtcheck", true /* daemon */));
    private final PreparedStatement healthCheckStatement;

    public static AsyncClusterSessionImpl create(String clusterName, CassandraClusterSessionPair pair,
            ThreadFactory threadFactory) {
        return create(clusterName, pair, Executors.newCachedThreadPool(threadFactory));
    }

    public static AsyncClusterSessionImpl create(String clusterName, CassandraClusterSessionPair pair,
            Executor executor) {
        PreparedStatement healthCheckStatement = pair.session().prepare("SELECT dateof(now()) FROM system.local ;");
        return new AsyncClusterSessionImpl(clusterName, pair, executor, healthCheckStatement);
    }

    private AsyncClusterSessionImpl(String sessionName, CassandraClusterSessionPair pair, Executor executor,
            PreparedStatement healthCheckStatement) {
        this.sessionName = sessionName;
        this.pair = pair;
        this.executor = executor;
        this.healthCheckStatement = healthCheckStatement;
    }

    @Override
    public Map<MetricName, Metric> usedCqlClusterMetrics() {
        return KeyedStream.stream(
                pair.cluster().getMetrics()
                        .getRegistry()
                        .getMetrics())
                .mapKeys(name -> MetricName.builder().safeName(name).build())
                .collectToMap();
    }

    @Nonnull
    @Override
    public String sessionName() {
        return sessionName;
    }

    @Override
    public void close() {
        service.shutdownNow();
        log.info("Shutting down health checker for cluster session {}", SafeArg.of("clusterSession", sessionName));
        boolean shutdown = false;
        try {
            shutdown = service.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.error("Interrupted while shutting down health checker. Should not happen");
            Thread.currentThread().interrupt();
        } finally {
            AsyncSessionManager.getOrInitializeAsyncSessionManager().closeClusterSession(this);
        }

        if (!shutdown) {
            log.error("Failed to shutdown health checker in a timely manner for {}",
                    SafeArg.of("clusterSession", sessionName));
        } else {
            log.info("Shut down health checker for cluster session {}", SafeArg.of("clusterSession", sessionName));
        }
    }

    @Override
    public  void start() {
        service.scheduleAtFixedRate(() -> {
            ListenableFuture<String> time = getTimeAsync();
            try {
                log.info("Current cluster time is: {}", SafeArg.of("clusterTime", time.get()));
            } catch (Exception e) {
                log.info("Cluster session health check failed");
            }
        }, 0, 1, TimeUnit.MINUTES);
    }

    private ListenableFuture<String> getTimeAsync() {
        return Futures.transform(pair.session().executeAsync(healthCheckStatement.bind()),
                result -> {
                    Row row;
                    StringBuilder builder = new StringBuilder();
                    while ((row = result.one()) != null) {
                        builder.append(row.getTimestamp(0));
                    }
                    return builder.toString();
                }, executor);
    }
}
