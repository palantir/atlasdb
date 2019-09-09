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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Metric;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.SafeArg;
import com.palantir.tritium.metrics.registry.MetricName;

public final class AsyncClusterSessionImpl implements AsyncClusterSession {

    private static final Logger log = LoggerFactory.getLogger(AsyncClusterSessionImpl.class);

    private final String sessionName;
    private final Executor executor;

    private final ScheduledExecutorService healthCheckExecutor = PTExecutors.newSingleThreadScheduledExecutor(
            new NamedThreadFactory(sessionName() + "-healthCheck", true /* daemon */));
    private final PreparedStatement healthCheckStatement;
    private final Cluster cluster;
    private final Session session;

    public static AsyncClusterSessionImpl create(String clusterName, Cluster cluster, Session session,
            ThreadFactory threadFactory) {
        return create(clusterName, cluster, session, Executors.newCachedThreadPool(threadFactory));
    }

    public static AsyncClusterSessionImpl create(String clusterName, Cluster cluster, Session session,
            Executor executor) {
        PreparedStatement healthCheckStatement = session.prepare("SELECT dateof(now()) FROM system.local ;");
        return new AsyncClusterSessionImpl(clusterName, cluster, session, executor, healthCheckStatement).start();
    }

    private AsyncClusterSessionImpl(String sessionName, Cluster cluster, Session session, Executor executor,
            PreparedStatement healthCheckStatement) {
        this.sessionName = sessionName;
        this.cluster = cluster;
        this.session = session;
        this.executor = executor;
        this.healthCheckStatement = healthCheckStatement;
    }

    @Override
    public Map<MetricName, Metric> usedCqlClusterMetrics() {
        return KeyedStream.stream(
                cluster.getMetrics()
                        .getRegistry()
                        .getMetrics())
                .mapKeys(name -> MetricName.builder().safeName(name).build())
                .collectToMap();
    }

    @Override
    public String sessionName() {
        return sessionName;
    }

    public void close() {
        healthCheckExecutor.shutdownNow();
        log.info("Shutting down health checker for cluster session {}", SafeArg.of("clusterSession", sessionName));
        boolean shutdown = false;
        try {
            shutdown = healthCheckExecutor.awaitTermination(5, TimeUnit.SECONDS);
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

    private AsyncClusterSessionImpl start() {
        healthCheckExecutor.scheduleAtFixedRate(() -> {
            ListenableFuture<String> time = getTimeAsync();
            try {
                log.info("Current cluster time is: {}", SafeArg.of("clusterTime", time.get()));
            } catch (Exception e) {
                log.info("Cluster session health check failed");
            }
        }, 0, 1, TimeUnit.MINUTES);
        return this;
    }

    private ListenableFuture<String> getTimeAsync() {
        return Futures.transform(session.executeAsync(healthCheckStatement.bind()),
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
