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

package com.palantir.atlasdb.keyvalue.cassandra.cql;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

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
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ThreadingOptions;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.LatencyAwarePolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.policies.WhiteListPolicy;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.common.streams.KeyedStream;
import com.palantir.conjure.java.config.ssl.SslSocketFactories;
import com.palantir.logsafe.SafeArg;
import com.palantir.tritium.metrics.registry.MetricName;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

public final class CQLSessionFactory {

    private static final Logger log = LoggerFactory.getLogger(CQLSessionFactory.class);
    private static final AtomicReference<CQLSessionFactory> SINGLETON = new AtomicReference<>(null);

    // TODO(jakubk): P3 Add some sort of reference counting to turn off the cluster
    private final Cache<UniqueCassandraCluster, ListenableFuture<ClusterSession>> clusters =
            Caffeine.newBuilder().build();
    private final AtomicLong cassandraId = new AtomicLong();

    private final TaggedMetricRegistry metrics;

    private CQLSessionFactory(TaggedMetricRegistry metrics) {
        this.metrics = metrics;
    }

    public static synchronized void initialize(TaggedMetricRegistry metricRegistry) {
        Preconditions.checkState(SINGLETON.get() == null, "Already initialized");
        SINGLETON.set(new CQLSessionFactory(metricRegistry));
    }

    public static CQLSessionFactory getInstance() {
        CQLSessionFactory instance = SINGLETON.get();
        Preconditions.checkState(instance != null, "Not initialized");
        return instance;
    }

    public ListenableFuture<ClusterSession> getSession(CassandraKeyValueServiceConfig config) {
        return clusters.get(ImmutableUniqueCassandraCluster.of(config.servers()),
                $ -> Futures.transform(createSession(config), clusterSession -> {
                    log.info("Created cluster", SafeArg.of("clusterId", clusterSession.clusterId()));
                    Session session = clusterSession.session();
                    String clusterId = clusterSession.clusterId();
                    metrics.addMetrics("cassandra-id", clusterId,
                            () -> KeyedStream.stream(
                                    session.getCluster().getMetrics()
                                            .getRegistry()
                                            .getMetrics())
                                    .mapKeys(name -> MetricName.builder().safeName(name).build())
                                    .collectToMap());

                    /* TODO(jakubk): I feel like the AtlasDB processing for sure should be in a separate thread; */
                    /* This stuff I'm not sure, but let's go with this for now */

                    ThreadFactory threadFactory = new ThreadFactoryBuilder()
                            .setNameFormat(clusterId + "-%d")
                            //                    .setUncaughtExceptionHandler(new ExecutorUncaughtExceptionHandler(
                            //                            name,
                            //                            throwable -> uncaughtErrors.uncaughtThrowableListener(name.get(), throwable)))
                            .build();

                    Executor callbackExecutor = Executors.newCachedThreadPool(threadFactory);
                    return ImmutableClusterSession.of(session, callbackExecutor);
                }, MoreExecutors.directExecutor()));
    }

    private ListenableFuture<CassandraCluster> createSession(CassandraKeyValueServiceConfig config) {
        long curId = cassandraId.getAndIncrement();
        String clusterName = "cassandra" + (curId == 0 ? "" : "-" + curId);

        log.info("Creating cluster", SafeArg.of("clusterId", clusterName));

        Cluster.Builder clusterBuilder = Cluster.builder();
        clusterBuilder.addContactPointsWithPorts(contactPoints(config));
        clusterBuilder.withClusterName(clusterName); // for JMX metrics
        clusterBuilder.withCredentials(config.credentials().username(), config.credentials().password());
        clusterBuilder.withCompression(ProtocolOptions.Compression.LZ4);
        clusterBuilder.withLoadBalancingPolicy(loadBalancingPolicy(config));
        clusterBuilder.withPoolingOptions(poolingOptions(config));
        clusterBuilder.withQueryOptions(queryOptions(config));
        clusterBuilder.withRetryPolicy(retryPolicy(config));
        clusterBuilder.withSSL(sslOptions(config));
        clusterBuilder.withThreadingOptions(new ThreadingOptions());

        Cluster cluster = createCluster(clusterBuilder);

        return Futures.transform(cluster.connectAsync(), session -> {
            return ImmutableCassandraCluster.of(session, clusterName);
        }, MoreExecutors.directExecutor());
    }

    private static Collection<InetSocketAddress> contactPoints(CassandraKeyValueServiceConfig config) {
        return config.servers().stream().map(
                address -> new InetSocketAddress(address.getHostName(), 9042)).collect(
                Collectors.toList());
    }

    private static SSLOptions sslOptions(CassandraKeyValueServiceConfig config) {
        if (config.sslConfiguration().isPresent()) {
            SSLContext sslContext = SslSocketFactories.createSslContext(config.sslConfiguration().get());
            SSLOptions sslOptions = RemoteEndpointAwareJdkSSLOptions.builder()
                    .withSSLContext(sslContext)
                    .build();
            return sslOptions;
        } else if (config.ssl().isPresent() && config.ssl().get()) {
            return RemoteEndpointAwareJdkSSLOptions.builder().build();
        } else {
            return null;
        }
    }

    private static PoolingOptions poolingOptions(CassandraKeyValueServiceConfig config) {
        PoolingOptions poolingOptions = new PoolingOptions();
        poolingOptions.setMaxRequestsPerConnection(HostDistance.LOCAL, config.poolSize());
        poolingOptions.setMaxRequestsPerConnection(HostDistance.REMOTE, config.poolSize());
        poolingOptions.setPoolTimeoutMillis(config.cqlPoolTimeoutMillis());
        return poolingOptions;
    }

    private static QueryOptions queryOptions(CassandraKeyValueServiceConfig config) {
        QueryOptions queryOptions = new QueryOptions();
        queryOptions.setFetchSize(config.fetchBatchCount());
        return queryOptions;
    }

    private static LoadBalancingPolicy loadBalancingPolicy(CassandraKeyValueServiceConfig config) {
        // Refuse to talk to nodes twice as (latency-wise) slow as the best one, over a timescale of 100ms,
        // and every 10s try to re-evaluate ignored nodes performance by giving them queries again.
        // Note we are being purposely datacenter-irreverent here, instead relying on latency alone
        // to approximate what DCAwareRR would do;
        // this is because DCs for Atlas are always quite latency-close and should be used this way,
        // not as if we have some cross-country backup DC.
        LoadBalancingPolicy policy = LatencyAwarePolicy.builder(new RoundRobinPolicy()).build();

        // If user wants, do not automatically add in new nodes to pool (useful during DC migrations / rebuilds)
        if (!config.autoRefreshNodes()) {
            policy = new WhiteListPolicy(policy, config.servers());
        }

        // also try and select coordinators who own the data we're talking about to avoid an extra hop,
        // but also shuffle which replica we talk to for a load balancing that comes at the expense
        // of less effective caching
        return new TokenAwarePolicy(policy, true);
    }

    private static RetryPolicy retryPolicy(CassandraKeyValueServiceConfig config) {
        return DefaultRetryPolicy.INSTANCE;
    }

    private static Cluster createCluster(Cluster.Builder clusterBuilder) {
        Cluster cluster;
        //        Metadata metadata;
        try {
            cluster = clusterBuilder.build();
            //            metadata = cluster.getMetadata(); // special; this is the first place we connect to
            // hosts, this is where people will see failures
        } catch (NoHostAvailableException e) {
            if (e.getMessage().contains("Unknown compression algorithm")) {
                clusterBuilder.withCompression(ProtocolOptions.Compression.NONE);
                cluster = clusterBuilder.build();
                //                metadata = cluster.getMetadata();
            } else {
                throw e;
            }
        } catch (IllegalStateException e) {
            // god dammit datastax what did I do to _you_
            if (e.getMessage().contains("requested compression is not available")) {
                clusterBuilder.withCompression(ProtocolOptions.Compression.NONE);
                cluster = clusterBuilder.build();
                //                metadata = cluster.getMetadata();
            } else {
                throw e;
            }
        }
        return cluster;
    }

    @Value.Immutable
    public interface UniqueCassandraCluster {
        @Value.Parameter
        Set<InetSocketAddress> servers();
    }

    @Value.Immutable
    public interface CassandraCluster {
        @Value.Parameter
        Session session();
        @Value.Parameter
        String clusterId();
    }

    @Value.Immutable
    public interface ClusterSession {
        @Value.Parameter
        Session session();
        @Value.Parameter
        Executor callbackExecutor();
    }
}
