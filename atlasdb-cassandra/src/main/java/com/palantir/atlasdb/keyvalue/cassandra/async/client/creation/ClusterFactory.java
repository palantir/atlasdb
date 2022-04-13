/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra.async.client.creation;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.RemoteEndpointAwareJdkSSLOptions;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.ThreadingOptions;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.LatencyAwarePolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.policies.WhiteListPolicy;
import com.palantir.atlasdb.cassandra.CassandraCredentialsConfig;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs.CassandraServersConfig;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraConstants;
import com.palantir.conjure.java.api.config.ssl.SslConfiguration;
import com.palantir.conjure.java.config.ssl.SslSocketFactories;
import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import javax.net.ssl.SSLContext;
import org.immutables.value.Value;

public class ClusterFactory {
    private final Supplier<Cluster.Builder> cqlClusterBuilderFactory;

    public ClusterFactory(Supplier<Cluster.Builder> cqlClusterBuilderFactory) {
        this.cqlClusterBuilderFactory = cqlClusterBuilderFactory;
    }

    public Cluster constructCluster(
            CassandraClusterConfig cassandraClusterConfig, CassandraServersConfig cassandraServersConfig) {
        Set<InetSocketAddress> hosts = CassandraServersConfigs.getCqlHosts(cassandraServersConfig);
        return constructCluster(hosts, cassandraClusterConfig);
    }

    public Cluster constructCluster(Set<InetSocketAddress> servers, CassandraClusterConfig cassandraClusterConfig) {
        Cluster.Builder clusterBuilder = cqlClusterBuilderFactory
                .get()
                .addContactPointsWithPorts(servers)
                .withCredentials(
                        cassandraClusterConfig.credentials().username(),
                        cassandraClusterConfig.credentials().password())
                .withCompression(ProtocolOptions.Compression.LZ4)
                .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
                .withoutJMXReporting()
                .withProtocolVersion(CassandraConstants.DEFAULT_PROTOCOL_VERSION)
                .withThreadingOptions(new ThreadingOptions());

        clusterBuilder = withSslOptions(
                clusterBuilder, cassandraClusterConfig.usingSsl(), cassandraClusterConfig.sslConfiguration());
        clusterBuilder = withPoolingOptions(
                clusterBuilder, cassandraClusterConfig.poolSize(), cassandraClusterConfig.cqlPoolTimeoutMillis());
        clusterBuilder = withQueryOptions(clusterBuilder, cassandraClusterConfig.fetchBatchCount());
        clusterBuilder = withLoadBalancingPolicy(clusterBuilder, cassandraClusterConfig.autoRefreshNodes(), servers);
        clusterBuilder = withSocketOptions(clusterBuilder, cassandraClusterConfig.socketQueryTimeoutMillis());

        return clusterBuilder.build();
    }

    private static Cluster.Builder withSocketOptions(Cluster.Builder clusterBuilder, int socketQueryTimeoutMillis) {
        return clusterBuilder.withSocketOptions(new SocketOptions()
                .setConnectTimeoutMillis(socketQueryTimeoutMillis)
                .setReadTimeoutMillis(socketQueryTimeoutMillis));
    }

    private static Cluster.Builder withSslOptions(
            Cluster.Builder builder, boolean usingSsl, Optional<SslConfiguration> sslConfiguration) {
        if (!usingSsl) {
            return builder;
        }
        if (sslConfiguration.isPresent()) {
            SSLContext sslContext = SslSocketFactories.createSslContext(sslConfiguration.get());
            return builder.withSSL(RemoteEndpointAwareJdkSSLOptions.builder()
                    .withSSLContext(sslContext)
                    .build());
        }
        return builder.withSSL(RemoteEndpointAwareJdkSSLOptions.builder().build());
    }

    private static Cluster.Builder withPoolingOptions(Cluster.Builder builder, int poolSize, int cqlPoolTimeoutMillis) {
        return builder.withPoolingOptions(new PoolingOptions()
                .setMaxConnectionsPerHost(HostDistance.LOCAL, poolSize)
                .setMaxConnectionsPerHost(HostDistance.REMOTE, poolSize)
                .setPoolTimeoutMillis(cqlPoolTimeoutMillis));
    }

    private static Cluster.Builder withQueryOptions(Cluster.Builder builder, int fetchBatchCount) {
        return builder.withQueryOptions(new QueryOptions().setFetchSize(fetchBatchCount));
    }

    private static Cluster.Builder withLoadBalancingPolicy(
            Cluster.Builder builder, boolean autoRefreshNodes, Set<InetSocketAddress> servers) {
        // Refuse to talk to nodes twice as (latency-wise) slow as the best one, over a timescale of 100ms,
        // and every 10s try to re-evaluate ignored nodes performance by giving them queries again.
        //
        // The DCAware RR policy prevents auto-discovery of remote datacenter contact points in a multi-DC setup,
        // which we are using to orchestrate migrations across datacenters.  We don't expect the policy to be helpful
        // for latency improvements.
        //
        // Since the local DC isn't specified, it will get set based on the contact points we provide.
        LoadBalancingPolicy policy = LatencyAwarePolicy.builder(
                        DCAwareRoundRobinPolicy.builder().build())
                .build();

        // If user wants, do not automatically add in new nodes to pool (useful during DC migrations / rebuilds)
        if (!autoRefreshNodes) {
            policy = new WhiteListPolicy(policy, servers);
        }

        // also try and select coordinators who own the data we're talking about to avoid an extra hop,
        // but also shuffle which replica we talk to for a load balancing that comes at the expense
        // of less effective caching, default to TokenAwarePolicy.ReplicaOrdering.RANDOM childPolicy
        return builder.withLoadBalancingPolicy(new TokenAwarePolicy(policy));
    }

    @Value.Immutable
    public abstract static class CassandraClusterConfig {
        public abstract CassandraCredentialsConfig credentials();

        public abstract int socketQueryTimeoutMillis();

        public abstract boolean usingSsl();

        public abstract Optional<SslConfiguration> sslConfiguration();

        public abstract int poolSize();

        public abstract int cqlPoolTimeoutMillis();

        public abstract boolean autoRefreshNodes();

        public abstract int fetchBatchCount();

        public static ImmutableCassandraClusterConfig.Builder builder() {
            return ImmutableCassandraClusterConfig.builder();
        }

        public static CassandraClusterConfig of(CassandraKeyValueServiceConfig config) {
            return builder()
                    .autoRefreshNodes(config.autoRefreshNodes())
                    .cqlPoolTimeoutMillis(config.cqlPoolTimeoutMillis())
                    .poolSize(config.poolSize())
                    .socketQueryTimeoutMillis(config.socketQueryTimeoutMillis())
                    .credentials(config.credentials())
                    .fetchBatchCount(config.fetchBatchCount())
                    .usingSsl(config.usingSsl())
                    .sslConfiguration(config.sslConfiguration())
                    .build();
        }
    }
}
