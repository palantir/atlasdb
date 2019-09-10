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

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.net.ssl.SSLContext;

import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.RemoteEndpointAwareJdkSSLOptions;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ThreadingOptions;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.policies.AddressTranslator;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.LatencyAwarePolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.policies.WhiteListPolicy;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.common.base.Throwables;
import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.conjure.java.config.ssl.SslSocketFactories;
import com.palantir.logsafe.SafeArg;

public final class AsyncClusterSessionImpl implements AsyncClusterSession {

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

    @Value.Immutable
    interface UniqueCassandraCluster {
        @Value.Parameter
        Set<InetSocketAddress> servers();
    }

    @Value.Immutable
    interface CassandraClusterSession extends Closeable {
        @Value.Parameter
        Cluster cluster();
        @Value.Parameter
        Session session();
        @Value.Parameter
        String clusterSessionName();

        default void close() {
            session().close();
            session().close();
        }
    }

    private static final AtomicLong cassandraId = new AtomicLong();

    private static final Cache<UniqueCassandraCluster, CassandraClusterSession> clusters = Caffeine.newBuilder()
            .weakValues()
            .<UniqueCassandraCluster, CassandraClusterSession>removalListener((key, value, cause) -> value.close())
            .build();

    // called in a thread safe manner to prevent multiple clusters and sessions over same servers
    private static CassandraClusterSession createClusterSession(CassandraKeyValueServiceConfig config,
            Set<InetSocketAddress> servers) {
        Cluster cluster = createCluster(config, servers);

        String sessionName = cluster.getClusterName()
                + "-session";
        return ImmutableCassandraClusterSession.builder()
                .cluster(cluster)
                .session(cluster.connect())
                .clusterSessionName(sessionName)
                .build();
    }


    private static Cluster createCluster(CassandraKeyValueServiceConfig config, Set<InetSocketAddress> servers) {
        long curId = cassandraId.getAndIncrement();
        String clusterName = "cassandra" + "-" + curId;
        SimpleAddressTranslator mapper = new SimpleAddressTranslator(config);

        Cluster.Builder clusterBuilder = Cluster.builder()
                .addContactPointsWithPorts(servers)
                .withClusterName(clusterName)
                .withCredentials(config.credentials().username(), config.credentials().password())
                .withCompression(ProtocolOptions.Compression.LZ4)
                .withLoadBalancingPolicy(loadBalancingPolicy(config, servers))
                .withPoolingOptions(poolingOptions(config))
                .withQueryOptions(queryOptions(config))
                .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
                .withAddressTranslator(mapper)
                .withoutJMXReporting()
                .withThreadingOptions(new ThreadingOptions());

        sslOptions(config).ifPresent(clusterBuilder::withSSL);

        return buildCluster(clusterBuilder);
    }

    private static Optional<RemoteEndpointAwareJdkSSLOptions> sslOptions(CassandraKeyValueServiceConfig config) {
        if (config.sslConfiguration().isPresent()) {
            SSLContext sslContext = SslSocketFactories.createSslContext(config.sslConfiguration().get());
            return Optional.of(RemoteEndpointAwareJdkSSLOptions.builder()
                    .withSSLContext(sslContext)
                    .build());
        } else if (config.ssl().isPresent() && config.ssl().get()) {
            return Optional.of(RemoteEndpointAwareJdkSSLOptions.builder().build());
        } else {
            return Optional.empty();
        }
    }

    private static PoolingOptions poolingOptions(CassandraKeyValueServiceConfig config) {
        return new PoolingOptions()
                .setMaxRequestsPerConnection(HostDistance.LOCAL, config.poolSize())
                .setMaxRequestsPerConnection(HostDistance.REMOTE, config.poolSize())
                .setPoolTimeoutMillis(config.cqlPoolTimeoutMillis());
    }

    private static QueryOptions queryOptions(CassandraKeyValueServiceConfig config) {
        return new QueryOptions().setFetchSize(config.fetchBatchCount());
    }

    private static LoadBalancingPolicy loadBalancingPolicy(CassandraKeyValueServiceConfig config,
            Set<InetSocketAddress> servers) {
        // Refuse to talk to nodes twice as (latency-wise) slow as the best one, over a timescale of 100ms,
        // and every 10s try to re-evaluate ignored nodes performance by giving them queries again.
        // Note we are being purposely datacenter-irreverent here, instead relying on latency alone
        // to approximate what DCAwareRR would do;
        // this is because DCs for Atlas are always quite latency-close and should be used this way,
        // not as if we have some cross-country backup DC.
        LoadBalancingPolicy policy = LatencyAwarePolicy.builder(new RoundRobinPolicy()).build();

        // If user wants, do not automatically add in new nodes to pool (useful during DC migrations / rebuilds)
        if (!config.autoRefreshNodes()) {
            policy = new WhiteListPolicy(policy, servers);
        }

        // also try and select coordinators who own the data we're talking about to avoid an extra hop,
        // but also shuffle which replica we talk to for a load balancing that comes at the expense
        // of less effective caching
        return new TokenAwarePolicy(policy, TokenAwarePolicy.ReplicaOrdering.RANDOM);
    }

    private static Cluster buildCluster(Cluster.Builder clusterBuilder) {
        Cluster cluster;
        try {
            cluster = clusterBuilder.build();
        } catch (NoHostAvailableException e) {
            if (e.getMessage().contains("Unknown compression algorithm")) {
                clusterBuilder.withCompression(ProtocolOptions.Compression.NONE);
                cluster = clusterBuilder.build();
            } else {
                throw Throwables.throwUncheckedException(e);
            }
        } catch (IllegalStateException e) {
            if (e.getMessage().contains("requested compression is not available")) {
                clusterBuilder.withCompression(ProtocolOptions.Compression.NONE);
                cluster = clusterBuilder.build();
            } else {
                throw Throwables.throwUncheckedException(e);
            }
        }
        return cluster;
    }


    private final Logger log = LoggerFactory.getLogger(AsyncClusterSessionImpl.class);

    private final String sessionName;
    private final Executor executor;

    private final ScheduledExecutorService healthCheckExecutor = PTExecutors.newSingleThreadScheduledExecutor(
            new NamedThreadFactory(sessionName() + "-healthCheck", true /* daemon */));
    private final PreparedStatement healthCheckStatement;
    private final Cluster cluster;
    private final Session session;

    public static AsyncClusterSession create(CassandraKeyValueServiceConfig config, Set<InetSocketAddress> servers) {
        CassandraClusterSession cassandraClusterSession =
                clusters.get(ImmutableUniqueCassandraCluster.builder().addAllServers(servers).build(),
                        key -> createClusterSession(config, servers));

        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat(cassandraClusterSession.clusterSessionName() + "-%d")
                .build();

        return create(cassandraClusterSession.clusterSessionName(),
                cassandraClusterSession.cluster(),
                cassandraClusterSession.session(),
                threadFactory);
    }

    public static AsyncClusterSession create(String clusterName, Cluster cluster, Session session,
            ThreadFactory threadFactory) {
        return create(clusterName, cluster, session, Executors.newCachedThreadPool(threadFactory));
    }

    public static AsyncClusterSession create(String clusterName, Cluster cluster, Session session,
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
    public String sessionName() {
        return sessionName;
    }

    @Override
    public void close() {
        healthCheckExecutor.shutdownNow();
        log.info("Shutting down health checker for cluster session {}", SafeArg.of("clusterSession", sessionName));
        boolean shutdown = false;
        try {
            shutdown = healthCheckExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.error("Interrupted while shutting down health checker. Should not happen");
            Thread.currentThread().interrupt();
        }

        if (!shutdown) {
            log.error("Failed to shutdown health checker in a timely manner for {}",
                    SafeArg.of("clusterSession", sessionName));
        } else {
            log.info("Shut down health checker for cluster session {}", SafeArg.of("clusterSession", sessionName));
        }
    }

    @Override
    public AsyncClusterSessionImpl start() {
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
