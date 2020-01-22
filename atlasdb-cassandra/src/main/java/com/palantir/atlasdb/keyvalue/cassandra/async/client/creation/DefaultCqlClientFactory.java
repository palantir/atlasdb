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

package com.palantir.atlasdb.keyvalue.cassandra.async.client.creation;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.RemoteEndpointAwareJdkSSLOptions;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.ThreadingOptions;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.LatencyAwarePolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.policies.WhiteListPolicy;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs;
import com.palantir.atlasdb.keyvalue.cassandra.async.CqlClient;
import com.palantir.atlasdb.keyvalue.cassandra.async.CqlClientImpl;
import com.palantir.conjure.java.config.ssl.SslSocketFactories;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import javax.net.ssl.SSLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultCqlClientFactory implements CqlClientFactory {
    public static final CqlClientFactory DEFAULT = new DefaultCqlClientFactory();

    private static final Logger log = LoggerFactory.getLogger(DefaultCqlClientFactory.class);

    private final Supplier<Cluster.Builder> cqlClusterBuilderFactory;

    public DefaultCqlClientFactory(Supplier<Cluster.Builder> cqlClusterBuilderFactory) {
        this.cqlClusterBuilderFactory = cqlClusterBuilderFactory;
    }

    public DefaultCqlClientFactory() {
        this(Cluster::builder);
    }

    public Optional<CqlClient> constructClient(
            TaggedMetricRegistry taggedMetricRegistry,
            CassandraKeyValueServiceConfig config,
            boolean initializeAsync) {
        return config.servers().accept(new CassandraServersConfigs.Visitor<Optional<CqlClient>>() {
            @Override
            public Optional<CqlClient> visit(CassandraServersConfigs.DefaultConfig defaultConfig) {
                return Optional.empty();
            }

            @Override
            public Optional<CqlClient> visit(CassandraServersConfigs.CqlCapableConfig cqlCapableConfig) {
                if (!cqlCapableConfig.validateHosts()) {
                    log.warn("Your CQL capable config is wrong, the hosts for CQL and Thrift are not the same, using "
                            + "async API will be falling back to synchronous implementations.");
                    return Optional.empty();
                }

                Set<InetSocketAddress> servers = cqlCapableConfig.cqlHosts();

                Cluster.Builder clusterBuilder = cqlClusterBuilderFactory.get()
                        .addContactPointsWithPorts(servers)
                        .withCredentials(config.credentials().username(), config.credentials().password())
                        .withCompression(ProtocolOptions.Compression.LZ4)
                        .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
                        .withoutJMXReporting()
                        .withProtocolVersion(ProtocolVersion.V3)
                        .withThreadingOptions(new ThreadingOptions());

                clusterBuilder = withSslOptions(clusterBuilder, config);
                clusterBuilder = withPoolingOptions(clusterBuilder, config);
                clusterBuilder = withQueryOptions(clusterBuilder, config);
                clusterBuilder = withLoadBalancingPolicy(clusterBuilder, config, servers);
                clusterBuilder = withSocketOptions(clusterBuilder, config);

                return Optional.of(CqlClientImpl.create(
                        taggedMetricRegistry,
                        clusterBuilder.build(),
                        cqlCapableConfig.tuning(),
                        initializeAsync));
            }
        });
    }

    private Cluster.Builder withSocketOptions(Cluster.Builder clusterBuilder, CassandraKeyValueServiceConfig config) {
        return clusterBuilder.withSocketOptions(
                new SocketOptions().setReadTimeoutMillis(config.socketQueryTimeoutMillis()));
    }

    private static Cluster.Builder withSslOptions(Cluster.Builder builder, CassandraKeyValueServiceConfig config) {
        if (!config.usingSsl()) {
            return builder;
        }
        if (config.sslConfiguration().isPresent()) {
            SSLContext sslContext = SslSocketFactories.createSslContext(config.sslConfiguration().get());
            return builder.withSSL(RemoteEndpointAwareJdkSSLOptions.builder()
                    .withSSLContext(sslContext)
                    .build());
        }
        return builder.withSSL(RemoteEndpointAwareJdkSSLOptions.builder().build());
    }

    private static Cluster.Builder withPoolingOptions(Cluster.Builder builder, CassandraKeyValueServiceConfig config) {
        return builder.withPoolingOptions(
                new PoolingOptions()
                        .setMaxConnectionsPerHost(HostDistance.LOCAL, config.poolSize())
                        .setMaxConnectionsPerHost(HostDistance.REMOTE, config.poolSize())
                        .setPoolTimeoutMillis(config.cqlPoolTimeoutMillis()));
    }

    private static Cluster.Builder withQueryOptions(Cluster.Builder builder, CassandraKeyValueServiceConfig config) {
        return builder.withQueryOptions(new QueryOptions().setFetchSize(config.fetchBatchCount()));
    }

    private static Cluster.Builder withLoadBalancingPolicy(
            Cluster.Builder builder,
            CassandraKeyValueServiceConfig config,
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
        // of less effective caching, default to TokenAwarePolicy.ReplicaOrdering.RANDOM childPolicy
        return builder.withLoadBalancingPolicy(new TokenAwarePolicy(policy));
    }
}
