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
import com.palantir.conjure.java.api.config.service.HumanReadableDuration;
import com.palantir.conjure.java.api.config.ssl.SslConfiguration;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

abstract class ForwardingCassandraKeyValueServiceConfig implements CassandraKeyValueServiceConfig {

    protected abstract CassandraKeyValueServiceConfig delegate();

    @Override
    public CassandraServersConfig servers() {
        return delegate().servers();
    }

    @Override
    public Map<String, InetSocketAddress> addressTranslation() {
        return delegate().addressTranslation();
    }

    @Override
    public Optional<String> namespace() {
        return delegate().namespace();
    }

    @Override
    public int maxConnectionBurstSize() {
        return delegate().maxConnectionBurstSize();
    }

    @Override
    public double proportionConnectionsToCheckPerEvictionRun() {
        return delegate().proportionConnectionsToCheckPerEvictionRun();
    }

    @Override
    public int idleConnectionTimeoutSeconds() {
        return delegate().idleConnectionTimeoutSeconds();
    }

    @Override
    public int timeBetweenConnectionEvictionRunsSeconds() {
        return delegate().timeBetweenConnectionEvictionRunsSeconds();
    }

    @Override
    public int poolRefreshIntervalSeconds() {
        return delegate().poolRefreshIntervalSeconds();
    }

    @Override
    public int unresponsiveHostBackoffTimeSeconds() {
        return delegate().unresponsiveHostBackoffTimeSeconds();
    }

    @Override
    public int gcGraceSeconds() {
        return delegate().gcGraceSeconds();
    }

    @Override
    public double localHostWeighting() {
        return delegate().localHostWeighting();
    }

    @Override
    public Optional<HostLocation> overrideHostLocation() {
        return delegate().overrideHostLocation();
    }

    @Override
    public HumanReadableDuration timeoutOnConnectionTerminate() {
        return delegate().timeoutOnConnectionTerminate();
    }

    @Override
    public HumanReadableDuration timeoutOnConnectionBorrow() {
        return delegate().timeoutOnConnectionBorrow();
    }

    @Override
    public HumanReadableDuration timeoutOnPoolEvictionFailure() {
        return delegate().timeoutOnPoolEvictionFailure();
    }

    @Override
    public String getKeyspaceOrThrow() {
        return delegate().getKeyspaceOrThrow();
    }

    @Override
    public Optional<String> keyspace() {
        return delegate().keyspace();
    }

    @Override
    public CassandraCredentialsConfig credentials() {
        return delegate().credentials();
    }

    @Override
    public Optional<Boolean> ssl() {
        return delegate().ssl();
    }

    @Override
    public Optional<SslConfiguration> sslConfiguration() {
        return delegate().sslConfiguration();
    }

    @Override
    public CassandraAsyncKeyValueServiceFactory asyncKeyValueServiceFactory() {
        return delegate().asyncKeyValueServiceFactory();
    }

    @Override
    public Optional<Supplier<ExecutorService>> thriftExecutorServiceFactory() {
        return delegate().thriftExecutorServiceFactory();
    }

    @Override
    public int replicationFactor() {
        return delegate().replicationFactor();
    }

    @Override
    public int mutationBatchCount() {
        return delegate().mutationBatchCount();
    }

    @Override
    public int mutationBatchSizeBytes() {
        return delegate().mutationBatchSizeBytes();
    }

    @Override
    public int fetchBatchCount() {
        return delegate().fetchBatchCount();
    }

    @Override
    public boolean ignoreNodeTopologyChecks() {
        return delegate().ignoreNodeTopologyChecks();
    }

    @Override
    public boolean ignoreInconsistentRingChecks() {
        return delegate().ignoreInconsistentRingChecks();
    }

    @Override
    public boolean ignoreDatacenterConfigurationChecks() {
        return delegate().ignoreDatacenterConfigurationChecks();
    }

    @Override
    public boolean ignorePartitionerChecks() {
        return delegate().ignorePartitionerChecks();
    }

    @Override
    public boolean autoRefreshNodes() {
        return delegate().autoRefreshNodes();
    }

    @Override
    public boolean clusterMeetsNormalConsistencyGuarantees() {
        return delegate().clusterMeetsNormalConsistencyGuarantees();
    }

    @Override
    public int socketTimeoutMillis() {
        return delegate().socketTimeoutMillis();
    }

    @Override
    public int socketQueryTimeoutMillis() {
        return delegate().socketQueryTimeoutMillis();
    }

    @Override
    public int cqlPoolTimeoutMillis() {
        return delegate().cqlPoolTimeoutMillis();
    }

    @Override
    public int schemaMutationTimeoutMillis() {
        return delegate().schemaMutationTimeoutMillis();
    }

    @Override
    public int rangesConcurrency() {
        return delegate().rangesConcurrency();
    }

    @Override
    public Integer timestampsGetterBatchSize() {
        return delegate().timestampsGetterBatchSize();
    }

    @Override
    public Integer sweepReadThreads() {
        return delegate().sweepReadThreads();
    }

    @Override
    public Optional<CassandraJmxCompactionConfig> jmx() {
        return delegate().jmx();
    }

    @Override
    public String type() {
        return delegate().type();
    }

    @Override
    public int concurrentGetRangesThreadPoolSize() {
        return delegate().concurrentGetRangesThreadPoolSize();
    }

    @Override
    public int poolSize() {
        return delegate().poolSize();
    }

    @Override
    public int defaultGetRangesConcurrency() {
        return delegate().defaultGetRangesConcurrency();
    }

    @Override
    public boolean usingSsl() {
        return delegate().usingSsl();
    }
}
