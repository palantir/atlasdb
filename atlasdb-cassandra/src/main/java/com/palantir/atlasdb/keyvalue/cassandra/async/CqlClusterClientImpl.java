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

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.AddressTranslator;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.tracing.AsyncTracer;

public final class CqlClusterClientImpl implements CqlClusterClient {

    static class SimpleAddressTranslator implements AddressTranslator {

        private final Map<String, InetSocketAddress> mapper;

        SimpleAddressTranslator(CassandraKeyValueServiceConfig config) {
            this.mapper = config.addressTranslation();
        }

        @Override
        public void init(Cluster cluster) {
        }

        @Override
        public InetSocketAddress translate(InetSocketAddress address) {
            InetSocketAddress temp = mapper.getOrDefault(address.getHostString(), address);
            return new InetSocketAddress(temp.getAddress(), temp.getPort());
        }

        @Override
        public void close() {
        }
    }


    private final Logger log = LoggerFactory.getLogger(CqlClusterClientImpl.class);

    private final String clientName;
    private final Executor executor;

    private final ScheduledExecutorService healthCheckExecutor = PTExecutors.newSingleThreadScheduledExecutor(
            new NamedThreadFactory(sessionName() + "-healthCheck", true /* daemon */));
    private final PreparedStatement healthCheckStatement;
    private final Cluster cluster;
    private final Session session;

    public static CqlClusterClient create(String clientName, Cluster cluster, Session session) {
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat(clientName + "-%d")
                .build();

        return create(clientName, cluster, session, threadFactory);
    }

    public static CqlClusterClient create(String clientName, Cluster cluster, Session session,
            ThreadFactory threadFactory) {
        return create(clientName, cluster, session, Executors.newCachedThreadPool(threadFactory));
    }

    public static CqlClusterClient create(String clientName, Cluster cluster, Session session,
            Executor executor) {
        PreparedStatement healthCheckStatement = session.prepare("SELECT dateof(now()) FROM system.local ;");
        return new CqlClusterClientImpl(clientName, cluster, session, executor, healthCheckStatement).start();
    }

    private CqlClusterClientImpl(String clientName, Cluster cluster, Session session, Executor executor,
            PreparedStatement healthCheckStatement) {
        this.clientName = clientName;
        this.cluster = cluster;
        this.session = session;
        this.executor = executor;
        this.healthCheckStatement = healthCheckStatement;
    }


    @Override
    public String sessionName() {
        return clientName;
    }

    @Override
    public void close() {
        healthCheckExecutor.shutdownNow();
        log.info("Shutting down health checker for cluster session {}", SafeArg.of("clusterSession", clientName));
        boolean shutdown = false;
        try {
            shutdown = healthCheckExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.error("Interrupted while shutting down health checker. Should not happen");
            Thread.currentThread().interrupt();
        }

        if (!shutdown) {
            log.error("Failed to shutdown health checker in a timely manner for {}",
                    SafeArg.of("clusterSession", clientName));
        } else {
            log.info("Shut down health checker for cluster session {}", SafeArg.of("clusterSession", clientName));
        }
    }

    private CqlClusterClientImpl start() {
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

    // TODO (OStevan): make a prepared statement cache as per
    //  https://docs.datastax.com/en/developer/java-driver/3.7/manual/statements/prepared/
    @Override
    public PreparedStatement prepareStatement(String queryString) {
        return session.prepare(queryString);
    }

    @Override
    public <V, R> ListenableFuture<R> executeQuery(CqlQuery<V, R> cqlQuery) {
        Visitor<V, R> visitor = cqlQuery.createVisitor();
        return transform(Futures.transformAsync(session.executeAsync(cqlQuery.boundStatement()), iterate(visitor),
                executor), Visitor::result);
    }

    @Override
    public <V, P, R> ListenableFuture<R> executeQueries(Stream<CqlQuery<V, P>> inputStatementPairStream,
            Function<List<P>, R> transformer) {
        List<ListenableFuture<P>> allResults = inputStatementPairStream
                .map(this::executeQuery)
                .collect(Collectors.toList());

        return transform(Futures.allAsList(allResults), transformer);
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

    private <I, O> ListenableFuture<O> transform(ListenableFuture<I> input,
            Function<? super I, ? extends O> function) {
        AsyncTracer asyncTracer = new AsyncTracer();
        return Futures.transform(input, p -> asyncTracer.withTrace(() -> function.apply(p)),
                executor);
    }


    private <V, R> AsyncFunction<ResultSet, Visitor<V, R>> iterate(
            final Visitor<V, R> visitor) {
        return rs -> {
            Preconditions.checkArgument(rs != null, "ResultSet should not be null when iterating");
            int remainingInPage = rs.getAvailableWithoutFetching();

            visitor.visitResultSet(rs, remainingInPage);

            boolean wasLastPage = rs.getExecutionInfo().getPagingState() == null;
            if (wasLastPage) {
                return Futures.immediateFuture(visitor);
            } else {
                ListenableFuture<ResultSet> future = rs.fetchMoreResults();
                return Futures.transformAsync(future, iterate(visitor), executor);
            }
        };
    }
}
