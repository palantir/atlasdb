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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.NettyOptions;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ThreadingOptions;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.palantir.async.initializer.AsyncInitializer;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.common.concurrent.PTExecutors;

public final class CqlClientFactoryImpl extends AbstractCqlClientFactory {

    public static final CqlClientFactory DEFAULT = new CqlClientFactoryImpl();

    private CqlClientFactoryImpl() {
        // Use instance
    }

    @Override
    protected Cluster buildCluster(
            CassandraKeyValueServiceConfig config,
            Set<InetSocketAddress> servers,
            LoadBalancingPolicy loadBalancingPolicy,
            PoolingOptions poolingOptions,
            QueryOptions queryOptions,
            Optional<NettyOptions> nettyOptions,
            Optional<SSLOptions> sslOptions) {
        Cluster.Builder clusterBuilder = Cluster.builder()
                .addContactPointsWithPorts(servers)
                .withCredentials(config.credentials().username(), config.credentials().password())
                .withCompression(ProtocolOptions.Compression.LZ4)
                .withLoadBalancingPolicy(loadBalancingPolicy)
                .withPoolingOptions(poolingOptions)
                .withQueryOptions(queryOptions)
                .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
                .withoutJMXReporting()
                .withThreadingOptions(new ThreadingOptions());

        nettyOptions.ifPresent(clusterBuilder::withNettyOptions);

        sslOptions.ifPresent(clusterBuilder::withSSL);

        return clusterBuilder.build();
    }

    @Override
    protected CqlResourceHandle openResources(
            CassandraKeyValueServiceConfig config,
            Cluster cluster,
            boolean asyncResource) {
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat(cluster.getClusterName() + "-session" + "-%d")
                .build();
        return CqlResourceHandleImpl.create(PTExecutors.newCachedThreadPool(threadFactory), cluster, asyncResource);
    }

    private static final class CqlResourceHandleImpl implements CqlResourceHandle {

        private static final class InitializingWrapper extends AsyncInitializer
                implements AutoDelegate_CqlResourceHandle {

            private volatile CqlResourceHandle cqlResourceHandle;
            private final Cluster cluster;
            private final ExecutorService executorService;

            InitializingWrapper(ExecutorService executorService, Cluster cluster) {
                this.executorService = executorService;
                this.cluster = cluster;
            }

            @Override
            protected void tryInitialize() {
                cqlResourceHandle = new CqlResourceHandleImpl(executorService, cluster.connect());
            }

            @Override
            protected String getInitializingClassName() {
                return "CqlResourceHandle";
            }

            @Override
            public CqlResourceHandle delegate() {
                isInitialized();
                return cqlResourceHandle;
            }
        }

        private static final Logger log = LoggerFactory.getLogger(CqlResourceHandleImpl.class);

        private final ExecutorService executorService;
        private final Session session;

        static CqlResourceHandle create(ExecutorService executorService, Cluster cluster, boolean initializeAsync) {
            if (initializeAsync) {
                return new InitializingWrapper(executorService, cluster);
            }
            return new CqlResourceHandleImpl(executorService, cluster.connect());
        }

        private CqlResourceHandleImpl(ExecutorService executorService, Session session) {
            this.executorService = executorService;
            this.session = session;
        }

        @Override
        public Session session() {
            return session;
        }

        @Override
        public Executor executor() {
            return executorService;
        }

        @Override
        public void close() {
            try {
                executorService.shutdown();
                if (executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                    log.info("Executor service terminated properly.");
                } else {
                    log.warn("Executor service timed out before shutting down, shutting down forcefully");
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                log.warn("Thread was interrupted while waiting for executor service to terminate.", e);
            } catch (Exception e) {
                log.warn("Exception on executor service termination", e);
            }
            Cluster cluster = session.getCluster();
            session.close();
            cluster.close();
            log.info("Resource handle successfully closed");
        }
    }
}
