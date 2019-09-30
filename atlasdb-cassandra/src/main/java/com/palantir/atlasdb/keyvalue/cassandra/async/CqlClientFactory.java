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
import java.net.SocketAddress;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import javax.net.ssl.SSLContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.NettyOptions;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.RemoteEndpointAwareJdkSSLOptions;
import com.datastax.driver.core.ThreadingOptions;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.LatencyAwarePolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.policies.WhiteListPolicy;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs;
import com.palantir.conjure.java.config.ssl.SslSocketFactories;

import io.netty.channel.socket.SocketChannel;
import io.netty.handler.proxy.Socks5ProxyHandler;

public final class CqlClientFactory {

    private static final Logger logger = LoggerFactory.getLogger(CqlClientFactory.class);

    private CqlClientFactory() {

    }

    public static CqlClient constructClient(CassandraKeyValueServiceConfig config, boolean initializeAsync) {
        return config.servers().accept(new CassandraServersConfigs.Visitor<CqlClient>() {
            @Override
            public CqlClient visit(CassandraServersConfigs.DefaultConfig defaultConfig) {
                return new ThrowingCqlClientImpl();
            }

            @Override
            public CqlClient visit(CassandraServersConfigs.CqlCapableConfig cqlCapableConfig) {
                return createClient(
                        config,
                        cqlCapableConfig.cqlHosts(),
                        cqlCapableConfig.socksProxy(),
                        initializeAsync);
            }
        });
    }

    private static CqlClient createClient(
            CassandraKeyValueServiceConfig config,
            Set<InetSocketAddress> servers,
            Optional<SocketAddress> proxy,
            boolean initializeAsync) {

        Cluster.Builder clusterBuilder = Cluster.builder()
                .addContactPointsWithPorts(servers)
                .withCredentials(config.credentials().username(), config.credentials().password())
                .withCompression(ProtocolOptions.Compression.LZ4)
                .withLoadBalancingPolicy(loadBalancingPolicy(config, servers))
                .withPoolingOptions(poolingOptions(config))
                .withQueryOptions(queryOptions(config))
                .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
                .withoutJMXReporting()
                .withThreadingOptions(new ThreadingOptions());

        proxy.ifPresent(proxyAddress -> clusterBuilder.withNettyOptions(new SocksProxyNettyOptions(proxyAddress)));

        sslOptions(config).ifPresent(clusterBuilder::withSSL);

        Cluster cluster = clusterBuilder.build();

        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat(cluster.getClusterName() + "-session" + "-%d")
                .build();

        return CqlClientImpl.create(cluster, Executors.newCachedThreadPool(threadFactory), initializeAsync);
    }

    private static Optional<RemoteEndpointAwareJdkSSLOptions> sslOptions(CassandraKeyValueServiceConfig config) {
        if (config.sslConfiguration().isPresent()) {
            SSLContext sslContext = SslSocketFactories.createSslContext(config.sslConfiguration().get());
            return Optional.of(RemoteEndpointAwareJdkSSLOptions.builder()
                    .withSSLContext(sslContext)
                    .build());
        } else if (config.ssl().orElse(false)) {
            return Optional.of(RemoteEndpointAwareJdkSSLOptions.builder().build());
        }
        return Optional.empty();
    }

    private static PoolingOptions poolingOptions(CassandraKeyValueServiceConfig config) {
        return new PoolingOptions()
                .setMaxConnectionsPerHost(HostDistance.LOCAL, config.poolSize())
                .setMaxConnectionsPerHost(HostDistance.REMOTE, config.poolSize())
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
        // of less effective caching, default to TokenAwarePolicy.ReplicaOrdering.RANDOM childPolicy
        return new TokenAwarePolicy(policy);
    }

    private static final class SocksProxyNettyOptions extends NettyOptions {

        private final SocketAddress proxyAddress;

        SocksProxyNettyOptions(SocketAddress proxyAddress) {
            this.proxyAddress = proxyAddress;
        }

        @Override
        public void afterChannelInitialized(SocketChannel channel) {
            channel.pipeline().addFirst(new Socks5ProxyHandler(proxyAddress));
        }
    }
}
