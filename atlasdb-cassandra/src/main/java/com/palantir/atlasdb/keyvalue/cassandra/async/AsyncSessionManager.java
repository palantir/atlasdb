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
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import javax.net.ssl.SSLContext;

import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.RemoteEndpointAwareJdkSSLOptions;
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
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.common.base.Throwables;
import com.palantir.conjure.java.config.ssl.SslSocketFactories;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

public final class AsyncSessionManager {

    @Value.Immutable
    interface UniqueCassandraCluster {
        @Value.Parameter
        Set<InetSocketAddress> servers();

        default void check() {
            Preconditions.checkState(servers() != null, "UniqueCassandraCluster has null as "
                    + "servers value");
        }
    }

    /**
     * Creating one session per cluster, in this case the underlying session is shared between different C* KVS as long
     * as they are trying to connect to the same cluster.
     */
    @Value.Immutable
    interface CassandraClusterSession {
        @Value.Parameter
        Cluster cluster();
        @Value.Parameter
        Session session();
    }

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

    private static final Logger log = LoggerFactory.getLogger(AsyncSessionManager.class);
    private static final AtomicReference<AsyncSessionManager> FACTORY = new AtomicReference<>(null);

    public static void initialize(TaggedMetricRegistry taggedMetricRegistry) {
        Preconditions.checkState(
                null == FACTORY.getAndUpdate(
                        previous ->
                                previous == null ? new AsyncSessionManager(taggedMetricRegistry) : previous
                ),
                "Already initialized");
    }

    public static AsyncSessionManager getOrInitializeAsyncSessionManager() {
        return FACTORY.updateAndGet(previous -> {
            if (previous == null) {
                return new AsyncSessionManager();
            } else {
                return previous;
            }
        });
    }

    private final TaggedMetricRegistry taggedMetricRegistry;
    private final Cache<UniqueCassandraCluster, CassandraClusterSession> clusters = Caffeine.newBuilder()
            .weakValues()
            .removalListener(
                    (RemovalListener<UniqueCassandraCluster, CassandraClusterSession>) (key, value, cause) -> {
                        value.session().close();
                        value.cluster().close();
                    })
            .build();

    private final AtomicLong cassandraId = new AtomicLong();
    private final AtomicLong sessionId = new AtomicLong();

    private AsyncSessionManager(TaggedMetricRegistry taggedMetricRegistry) {
        this.taggedMetricRegistry = taggedMetricRegistry;
    }

    private AsyncSessionManager() {
        this.taggedMetricRegistry = null;
    }

    public AsyncClusterSession getAsyncSession(CassandraKeyValueServiceConfig config, Set<InetSocketAddress> servers) {
        CassandraClusterSession cassandraClusterSession = getCassandraClusterSession(config, servers);

        long curId = sessionId.getAndIncrement();
        String sessionName = cassandraClusterSession.cluster().getClusterName()
                + "-session" + "-" + curId;

        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat(sessionName + "-%d")
                .build();

        return AsyncClusterSessionImpl.create(sessionName, cassandraClusterSession, threadFactory);
    }

    public void closeClusterSession(AsyncClusterSession asyncClusterSession) {
        if (!(asyncClusterSession instanceof AsyncClusterSessionImpl)) {
            log.warn("Closing session which was not opened by this manager");
        }
    }

    private CassandraClusterSession getCassandraClusterSession(CassandraKeyValueServiceConfig config,
            Set<InetSocketAddress> servers) {
        return clusters.get(ImmutableUniqueCassandraCluster.of(servers),
                key -> createCassandraClusterSession(config, servers));
    }

    private CassandraClusterSession createCassandraClusterSession(CassandraKeyValueServiceConfig config,
            Set<InetSocketAddress> servers) {
        Cluster cluster = createCluster(config, servers);
        Session session;

        try {
            session = cluster.connectAsync().get();
        } catch (Exception e) {
            log.warn("Error on cluster connection");
            throw Throwables.unwrapAndThrowAtlasDbDependencyException(e);
        }

        return ImmutableCassandraClusterSession.of(cluster, Objects.requireNonNull(session));
    }


    private Cluster createCluster(CassandraKeyValueServiceConfig config, Set<InetSocketAddress> servers) {
        long curId = cassandraId.getAndIncrement();
        String clusterName = "cassandra" + "-" + curId;

        log.info("Creating cluster {}", SafeArg.of("clusterId", clusterName));

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
}
