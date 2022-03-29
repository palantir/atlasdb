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

import static com.palantir.logsafe.Preconditions.checkArgument;

import com.palantir.atlasdb.cassandra.CassandraServersConfigs.CassandraServersConfig;
import com.palantir.atlasdb.keyvalue.cassandra.async.CassandraAsyncKeyValueServiceFactory;
import com.palantir.atlasdb.keyvalue.cassandra.pool.HostLocation;
import com.palantir.atlasdb.spi.SharedResourcesConfig;
import com.palantir.conjure.java.api.config.service.HumanReadableDuration;
import com.palantir.conjure.java.api.config.ssl.SslConfiguration;
import com.palantir.refreshable.Refreshable;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import org.immutables.value.Value.Default;

public class CassandraReloadableKvsConfig implements MergedCassandraKeyValueServiceConfig {

    private final CassandraKeyValueServiceConfig config;
    private final Supplier<CassandraKeyValueServiceRuntimeConfig> runtimeConfigSupplier;

    public CassandraReloadableKvsConfig(
            CassandraKeyValueServiceConfig config,
            Refreshable<CassandraKeyValueServiceRuntimeConfig> runtimeConfigRefreshable) {
        this.config = config;
        this.runtimeConfigSupplier = runtimeConfigRefreshable.map(runtimeConfig -> {
            checkArgument(
                    servers(config, runtimeConfig).numberOfThriftHosts() > 0,
                    "'servers' must have at least one defined host");

            checkArgument(
                    replicationFactor(config, runtimeConfig).isPresent(),
                    "'replicationFactor' must be set to a non-negative value");

            return runtimeConfig;
        });
    }

    @Override
    public CassandraServersConfig servers() {
        return servers(config, runtimeConfigSupplier.get());
    }

    private static CassandraServersConfig servers(
            CassandraKeyValueServiceConfig config, CassandraKeyValueServiceRuntimeConfig runtimeConfig) {
        return config.servers().orElseGet(runtimeConfig::servers);
    }

    @Override
    public int unresponsiveHostBackoffTimeSeconds() {
        return config.unresponsiveHostBackoffTimeSeconds().orElseGet(() -> runtimeConfigSupplier.get().unresponsiveHostBackoffTimeSeconds());
    }

    @Override
    public int mutationBatchCount() {
        return config.mutationBatchCount().orElseGet(() -> runtimeConfigSupplier.get().mutationBatchCount());
    }

    @Override
    public int mutationBatchSizeBytes() {
        return config.mutationBatchSizeBytes().orElseGet(() -> runtimeConfigSupplier.get().mutationBatchSizeBytes());
    }

    @Override
    public int fetchBatchCount() {
        return config.fetchBatchCount().orElseGet(() -> runtimeConfigSupplier.get().fetchBatchCount());
    }

    @Override
    public int sweepReadThreads() {
        return config.sweepReadThreads().orElseGet(() -> runtimeConfigSupplier.get().sweepReadThreads());
    }

    @Default
    @Override
    public int concurrentGetRangesThreadPoolSize() {
        if (config.servers().isPresent()) {
            return config.concurrentGetRangesThreadPoolSize();
        }

        // Use the initial number of thrift hosts as a best guess
        return poolSize() * servers().numberOfThriftHosts();
    }

    @Override
    public Optional<SharedResourcesConfig> sharedResourcesConfig() {
        return config.sharedResourcesConfig();
    }

    @Override
    public CassandraCellLoadingConfig cellLoadingConfig() {
        return runtimeConfigSupplier.get().cellLoadingConfig();
    }

    @Override
    public int numberOfRetriesOnSameHost() {
        return runtimeConfigSupplier.get().numberOfRetriesOnSameHost();
    }

    @Override
    public int numberOfRetriesOnAllHosts() {
        return runtimeConfigSupplier.get().numberOfRetriesOnAllHosts();
    }

    @Override
    public int fetchReadLimitPerRow() {
        return runtimeConfigSupplier.get().fetchReadLimitPerRow();
    }

    @Override
    public boolean conservativeRequestExceptionHandler() {
        return runtimeConfigSupplier.get().conservativeRequestExceptionHandler();
    }

    @Override
    public CassandraTracingConfig tracing() {
        return runtimeConfigSupplier.get().tracing();
    }

    @Override
    public int defaultGetRangesConcurrency() {
        if (config.servers().isPresent()) {
            return config.defaultGetRangesConcurrency();
        }

        return Math.min(8, concurrentGetRangesThreadPoolSize() / 2);
    }

    @Override
    public int replicationFactor() {
        // Safety: If #replicationFactor would return an optional, then the precondition in the runtimeConfigSupplier
        // would have thrown, reverting to a previously valid config where an optional would not be thrown.
        return replicationFactor(config, runtimeConfigSupplier.get()).orElseThrow();
    }

    private static Optional<Integer> replicationFactor(
            CassandraKeyValueServiceConfig config, CassandraKeyValueServiceRuntimeConfig runtimeConfig) {
        return config.replicationFactor().or(runtimeConfig::replicationFactor);
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
        return config.poolSize();
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
    public int gcGraceSeconds() {
        return config.gcGraceSeconds();
    }

    @Override
    public double localHostWeighting() {
        return config.localHostWeighting();
    }

    @Override
    public int consecutiveAbsencesBeforePoolRemoval() {
        return config.consecutiveAbsencesBeforePoolRemoval();
    }

    @Override
    public Optional<HostLocation> overrideHostLocation() {
        return config.overrideHostLocation();
    }

    @Override
    public Duration timeoutOnConnectionClose() {
        return config.timeoutOnConnectionClose();
    }

    @Override
    public HumanReadableDuration timeoutOnConnectionBorrow() {
        return config.timeoutOnConnectionBorrow();
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
    public int initialSocketQueryTimeoutMillis() {
        return config.initialSocketQueryTimeoutMillis();
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
    public Optional<CassandraJmxCompactionConfig> jmx() {
        return config.jmx();
    }

    @Override
    public String type() {
        return config.type();
    }

    @Override
    public Integer timestampsGetterBatchSize() {
        return config.timestampsGetterBatchSize();
    }
}
