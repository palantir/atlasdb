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

import com.google.common.base.MoreObjects;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs.CassandraServersConfig;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs.ThriftHostsExtractingVisitor;
import com.palantir.atlasdb.keyvalue.cassandra.async.CassandraAsyncKeyValueServiceFactory;
import com.palantir.atlasdb.keyvalue.cassandra.pool.HostLocation;
import com.palantir.atlasdb.spi.KeyValueServiceRuntimeConfig;
import com.palantir.conjure.java.api.config.ssl.SslConfiguration;
import com.palantir.logsafe.Preconditions;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;

public class CassandraReloadableKvsConfig implements CassandraKeyValueServiceConfig {
    private final CassandraKeyValueServiceConfig config;
    private final Supplier<Optional<KeyValueServiceRuntimeConfig>> runtimeConfigSupplier;

    public CassandraReloadableKvsConfig(
            CassandraKeyValueServiceConfig config, Supplier<Optional<KeyValueServiceRuntimeConfig>> runtimeConfig) {
        this.config = config;
        this.runtimeConfigSupplier = runtimeConfig;
    }

    @Override
    public CassandraServersConfigs.CassandraServersConfig servers() {
        // get servers from install config (for backcompat), otherwise get servers from runtime config
        if (config.servers().numberOfThriftHosts() > 0) {
            return config.servers();
        }

        CassandraServersConfig servers = chooseConfig(CassandraKeyValueServiceRuntimeConfig::servers, config.servers());
        Preconditions.checkState(
                !servers.accept(new ThriftHostsExtractingVisitor()).isEmpty(),
                "'servers' must have at least one defined host");
        return servers;
    }

    @Override
    public Map<String, InetSocketAddress> addressTranslation() {
        return config.addressTranslation();
    }

    @Override
    public Optional<String> namespace() {
        return config.namespace();
    }

    @Override
    public int poolSize() {
        return chooseConfig(CassandraKeyValueServiceRuntimeConfig::poolSize, config.poolSize());
    }

    @Override
    public int maxConnectionBurstSize() {
        return config.maxConnectionBurstSize();
    }

    @Override
    public double proportionConnectionsToCheckPerEvictionRun() {
        return config.proportionConnectionsToCheckPerEvictionRun();
    }

    @Override
    public int idleConnectionTimeoutSeconds() {
        return config.idleConnectionTimeoutSeconds();
    }

    @Override
    public int timeBetweenConnectionEvictionRunsSeconds() {
        return config.timeBetweenConnectionEvictionRunsSeconds();
    }

    @Override
    public int poolRefreshIntervalSeconds() {
        return config.poolRefreshIntervalSeconds();
    }

    @Override
    public int unresponsiveHostBackoffTimeSeconds() {
        return chooseConfig(
                CassandraKeyValueServiceRuntimeConfig::unresponsiveHostBackoffTimeSeconds,
                config.unresponsiveHostBackoffTimeSeconds());
    }

    @Override
    public int gcGraceSeconds() {
        return config.gcGraceSeconds();
    }

    @Override
    public double localHostWeighting() {
        return config.localHostWeighting();
    }

    @Override
    public Optional<HostLocation> overrideHostLocation() {
        return config.overrideHostLocation();
    }

    @Override
    public String getKeyspaceOrThrow() {
        return config.getKeyspaceOrThrow();
    }

    @Override
    public Optional<String> keyspace() {
        return config.keyspace();
    }

    @Override
    public CassandraCredentialsConfig credentials() {
        return config.credentials();
    }

    @Override
    public Optional<Boolean> ssl() {
        return config.ssl();
    }

    @Override
    public Optional<SslConfiguration> sslConfiguration() {
        return config.sslConfiguration();
    }

    @Override
    public CassandraAsyncKeyValueServiceFactory asyncKeyValueServiceFactory() {
        return config.asyncKeyValueServiceFactory();
    }

    @Override
    public Optional<Supplier<ExecutorService>> thriftExecutorServiceFactory() {
        return config.thriftExecutorServiceFactory();
    }

    @Override
    public int replicationFactor() {
        return config.replicationFactor();
    }

    @Override
    public int mutationBatchCount() {
        return chooseConfig(CassandraKeyValueServiceRuntimeConfig::mutationBatchCount, config.mutationBatchCount());
    }

    @Override
    public int mutationBatchSizeBytes() {
        return chooseConfig(
                CassandraKeyValueServiceRuntimeConfig::mutationBatchSizeBytes, config.mutationBatchSizeBytes());
    }

    @Override
    public int fetchBatchCount() {
        return chooseConfig(CassandraKeyValueServiceRuntimeConfig::fetchBatchCount, config.fetchBatchCount());
    }

    @Override
    public boolean ignoreNodeTopologyChecks() {
        return config.ignoreNodeTopologyChecks();
    }

    @Override
    public boolean ignoreInconsistentRingChecks() {
        return config.ignoreInconsistentRingChecks();
    }

    @Override
    public boolean ignoreDatacenterConfigurationChecks() {
        return config.ignoreDatacenterConfigurationChecks();
    }

    @Override
    public boolean ignorePartitionerChecks() {
        return config.ignorePartitionerChecks();
    }

    @Override
    public boolean autoRefreshNodes() {
        return config.autoRefreshNodes();
    }

    @Override
    public boolean clusterMeetsNormalConsistencyGuarantees() {
        return config.clusterMeetsNormalConsistencyGuarantees();
    }

    @Override
    public int socketTimeoutMillis() {
        return config.socketTimeoutMillis();
    }

    @Override
    public int socketQueryTimeoutMillis() {
        return config.socketQueryTimeoutMillis();
    }

    @Override
    public int cqlPoolTimeoutMillis() {
        return config.cqlPoolTimeoutMillis();
    }

    @Override
    public int schemaMutationTimeoutMillis() {
        return config.schemaMutationTimeoutMillis();
    }

    @Override
    public int rangesConcurrency() {
        return config.rangesConcurrency();
    }

    @Override
    public Integer timestampsGetterBatchSize() {
        return config.timestampsGetterBatchSize();
    }

    @Override
    public Integer sweepReadThreads() {
        return chooseConfig(CassandraKeyValueServiceRuntimeConfig::sweepReadThreads, config.sweepReadThreads());
    }

    @Override
    public Optional<CassandraJmxCompactionConfig> jmx() {
        return config.jmx();
    }

    @Override
    public String type() {
        return config.type();
    }

    @Override
    public int concurrentGetRangesThreadPoolSize() {
        return poolSize() * servers().numberOfThriftHosts();
    }

    @Override
    public int defaultGetRangesConcurrency() {
        return config.defaultGetRangesConcurrency();
    }

    @Override
    public boolean usingSsl() {
        return config.usingSsl();
    }

    private <T> T chooseConfig(Function<CassandraKeyValueServiceRuntimeConfig, T> runtimeConfig, T installConfig) {
        return MoreObjects.firstNonNull(unwrapRuntimeConfig(runtimeConfig), installConfig);
    }

    private <T> T unwrapRuntimeConfig(Function<CassandraKeyValueServiceRuntimeConfig, T> function) {
        Optional<KeyValueServiceRuntimeConfig> runtimeConfigOptional = runtimeConfigSupplier.get();
        if (!runtimeConfigOptional.isPresent()) {
            return null;
        }
        CassandraKeyValueServiceRuntimeConfig ckvsRuntimeConfig =
                (CassandraKeyValueServiceRuntimeConfig) runtimeConfigOptional.get();

        return function.apply(ckvsRuntimeConfig);
    }
}
