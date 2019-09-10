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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import javax.net.ssl.SSLContext;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.RemoteEndpointAwareJdkSSLOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ThreadingOptions;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.LatencyAwarePolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.policies.WhiteListPolicy;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.common.base.Throwables;
import com.palantir.conjure.java.config.ssl.SslSocketFactories;

public class CqlClientFactoryImpl implements CqlClientFactory {


    public CqlClient constructClient(CassandraKeyValueServiceConfig config) {
        return config.servers().visit((thriftServers, maybeCqlServers) -> maybeCqlServers).map(servers -> {
            CqlClientImpl.SimpleAddressTranslator mapper = new CqlClientImpl.SimpleAddressTranslator(config);

            Cluster.Builder clusterBuilder = Cluster.builder()
                    .addContactPointsWithPorts(servers)
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

            Cluster cluster = buildCluster(clusterBuilder);

            String sessionName = cluster.getClusterName()
                    + "-session";

            Session session = cluster.connect();

            return CqlClientFactoryImpl.create(sessionName, session);
        }).orElseGet(ThrowingCqlClientImpl::new);
    }

    public static CqlClient create(String clientName, Session session) {
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat(clientName + "-%d")
                .build();

        return create(clientName, session, threadFactory);
    }

    public static CqlClient create(String clientName, Session session,
            ThreadFactory threadFactory) {
        return create(clientName, session, Executors.newCachedThreadPool(threadFactory));
    }

    public static CqlClient create(String clientName, Session session,
            Executor executor) {
        return CqlClientImpl.create(clientName, session, executor);
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
}
