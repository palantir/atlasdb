/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.cassandra;

import com.palantir.atlasdb.cassandra.CassandraServersConfigs.CassandraServersConfig;
import com.palantir.atlasdb.keyvalue.cassandra.async.CassandraAsyncKeyValueServiceFactory;
import com.palantir.atlasdb.keyvalue.cassandra.pool.HostLocation;
import com.palantir.conjure.java.api.config.ssl.SslConfiguration;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

public final class DelegatingCassandraKeyValueServiceConfig implements CassandraKeyValueServiceConfig {

    private final Supplier<CassandraKeyValueServiceConfig> delegate;

    public DelegatingCassandraKeyValueServiceConfig(Supplier<CassandraKeyValueServiceConfig> delegate) {
        this.delegate = delegate;
    }

    @Override
    public CassandraServersConfig servers() {
        return delegate.get().servers();
    }

    @Override
    public Map<String, InetSocketAddress> addressTranslation() {
        return delegate.get().addressTranslation();
    }

    @Override
    public Optional<String> namespace() {
        return delegate.get().namespace();
    }

    @Override
    public int maxConnectionBurstSize() {
        return delegate.get().maxConnectionBurstSize();
    }

    @Override
    public double proportionConnectionsToCheckPerEvictionRun() {
        return delegate.get().proportionConnectionsToCheckPerEvictionRun();
    }

    @Override
    public int idleConnectionTimeoutSeconds() {
        return delegate.get().idleConnectionTimeoutSeconds();
    }

    @Override
    public int timeBetweenConnectionEvictionRunsSeconds() {
        return delegate.get().timeBetweenConnectionEvictionRunsSeconds();
    }

    @Override
    public int poolRefreshIntervalSeconds() {
        return delegate.get().poolRefreshIntervalSeconds();
    }

    @Override
    public int unresponsiveHostBackoffTimeSeconds() {
        return delegate.get().unresponsiveHostBackoffTimeSeconds();
    }

    @Override
    public int gcGraceSeconds() {
        return delegate.get().gcGraceSeconds();
    }

    @Override
    public double localHostWeighting() {
        return delegate.get().localHostWeighting();
    }

    @Override
    public Optional<HostLocation> overrideHostLocation() {
        return delegate.get().overrideHostLocation();
    }

    @Override
    public String getKeyspaceOrThrow() {
        return delegate.get().getKeyspaceOrThrow();
    }

    @Override
    public Optional<String> keyspace() {
        return delegate.get().keyspace();
    }

    @Override
    public CassandraCredentialsConfig credentials() {
        return delegate.get().credentials();
    }

    @Override
    public Optional<Boolean> ssl() {
        return delegate.get().ssl();
    }

    @Override
    public Optional<SslConfiguration> sslConfiguration() {
        return delegate.get().sslConfiguration();
    }

    @Override
    public CassandraAsyncKeyValueServiceFactory asyncKeyValueServiceFactory() {
        return delegate.get().asyncKeyValueServiceFactory();
    }

    @Override
    public Optional<Supplier<ExecutorService>> thriftExecutorServiceFactory() {
        return delegate.get().thriftExecutorServiceFactory();
    }

    @Override
    public int replicationFactor() {
        return delegate.get().replicationFactor();
    }

    @Override
    public int mutationBatchCount() {
        return delegate.get().mutationBatchCount();
    }

    @Override
    public int mutationBatchSizeBytes() {
        return delegate.get().mutationBatchSizeBytes();
    }

    @Override
    public int fetchBatchCount() {
        return delegate.get().fetchBatchCount();
    }

    @Override
    public boolean ignoreNodeTopologyChecks() {
        return delegate.get().ignoreNodeTopologyChecks();
    }

    @Override
    public boolean ignoreInconsistentRingChecks() {
        return delegate.get().ignoreInconsistentRingChecks();
    }

    @Override
    public boolean ignoreDatacenterConfigurationChecks() {
        return delegate.get().ignoreDatacenterConfigurationChecks();
    }

    @Override
    public boolean ignorePartitionerChecks() {
        return delegate.get().ignorePartitionerChecks();
    }

    @Override
    public boolean autoRefreshNodes() {
        return delegate.get().autoRefreshNodes();
    }

    @Override
    public boolean clusterMeetsNormalConsistencyGuarantees() {
        return delegate.get().clusterMeetsNormalConsistencyGuarantees();
    }

    @Override
    public int socketTimeoutMillis() {
        return delegate.get().socketTimeoutMillis();
    }

    @Override
    public int socketQueryTimeoutMillis() {
        return delegate.get().socketQueryTimeoutMillis();
    }

    @Override
    public int cqlPoolTimeoutMillis() {
        return delegate.get().cqlPoolTimeoutMillis();
    }

    @Override
    public int schemaMutationTimeoutMillis() {
        return delegate.get().schemaMutationTimeoutMillis();
    }

    @Override
    public int rangesConcurrency() {
        return delegate.get().rangesConcurrency();
    }

    @Override
    public Integer timestampsGetterBatchSize() {
        return delegate.get().timestampsGetterBatchSize();
    }

    @Override
    public Integer sweepReadThreads() {
        return delegate.get().sweepReadThreads();
    }

    @Override
    public Optional<CassandraJmxCompactionConfig> jmx() {
        return delegate.get().jmx();
    }

    @Override
    public String type() {
        return delegate.get().type();
    }

    @Override
    public int concurrentGetRangesThreadPoolSize() {
        return delegate.get().concurrentGetRangesThreadPoolSize();
    }

    @Override
    public int poolSize() {
        return delegate.get().poolSize();
    }

    @Override
    public int defaultGetRangesConcurrency() {
        return delegate.get().defaultGetRangesConcurrency();
    }

    @Override
    public boolean usingSsl() {
        return delegate.get().usingSsl();
    }
}
