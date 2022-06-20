/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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
import com.palantir.logsafe.SafeArg;
import com.palantir.refreshable.Refreshable;

public final class CassandraReloadableKeyValueServiceRuntimeConfig
        extends ForwardingCassandraKeyValueServiceRuntimeConfig {

    private final CassandraKeyValueServiceConfig installConfig;
    private final CassandraKeyValueServiceRuntimeConfig runtimeConfig;

    private CassandraReloadableKeyValueServiceRuntimeConfig(
            CassandraKeyValueServiceConfig installConfig, CassandraKeyValueServiceRuntimeConfig runtimeConfig) {
        this.installConfig = installConfig;
        this.runtimeConfig = runtimeConfig;
    }

    /**
     * Use {@link CassandraKeyValueServiceConfigs} to construct a reloading config where necessary.
     */
    static Refreshable<CassandraReloadableKeyValueServiceRuntimeConfig> fromConfigs(
            CassandraKeyValueServiceConfig installConfig,
            Refreshable<CassandraKeyValueServiceRuntimeConfig> runtimeConfigRefreshable) {
        return runtimeConfigRefreshable.map(runtimeConfig ->
                validate(new CassandraReloadableKeyValueServiceRuntimeConfig(installConfig, runtimeConfig)));
    }

    @Override
    public CassandraKeyValueServiceRuntimeConfig delegate() {
        return runtimeConfig;
    }

    @Override
    public CassandraServersConfig servers() {
        return installConfig.servers().orElseGet(runtimeConfig::servers);
    }

    @Override
    public int unresponsiveHostBackoffTimeSeconds() {
        return installConfig
                .unresponsiveHostBackoffTimeSeconds()
                .orElseGet(runtimeConfig::unresponsiveHostBackoffTimeSeconds);
    }

    @Override
    public int mutationBatchCount() {
        return installConfig.mutationBatchCount().orElseGet(runtimeConfig::mutationBatchCount);
    }

    @Override
    public int mutationBatchSizeBytes() {
        return installConfig.mutationBatchSizeBytes().orElseGet(runtimeConfig::mutationBatchSizeBytes);
    }

    @Override
    public int fetchBatchCount() {
        return installConfig.fetchBatchCount().orElseGet(runtimeConfig::fetchBatchCount);
    }

    @Override
    public Integer sweepReadThreads() {
        return installConfig.sweepReadThreads().orElseGet(runtimeConfig::sweepReadThreads);
    }

    @Override
    public Integer replicationFactor() {
        return installConfig.replicationFactor().orElseGet(runtimeConfig::replicationFactor);
    }

    public int concurrentGetRangesThreadPoolSize() {
        if (installConfig.concurrentGetRangesThreadPoolSize() > 0) {
            return installConfig.concurrentGetRangesThreadPoolSize();
        }
        return installConfig.poolSize() * servers().numberOfThriftHosts();
    }

    public int defaultGetRangesConcurrency() {
        if (installConfig.defaultGetRangesConcurrency() > 0) {
            return installConfig.defaultGetRangesConcurrency();
        }
        return Math.min(8, concurrentGetRangesThreadPoolSize() / 2);
    }

    private void checkPositiveNumberOfThriftHosts() {
        checkArgument(servers().numberOfThriftHosts() > 0, "'servers' must have at least one defined host");
    }

    private void checkNonNegativeReplicationFactor() {
        checkArgument(replicationFactor() >= 0, "'replicationFactor' must be non-negative");
    }

    private void checkSharedGetRangesPoolGreaterThanOrEqualToConcurrentGetRangesThreadPool() {
        installConfig
                .sharedResourcesConfig()
                .ifPresent(config -> checkArgument(
                        config.sharedGetRangesPoolSize() >= concurrentGetRangesThreadPoolSize(),
                        "If set, shared get ranges pool size must not be less than individual pool size.",
                        SafeArg.of("shared", config.sharedGetRangesPoolSize()),
                        SafeArg.of("individual", concurrentGetRangesThreadPoolSize())));
    }

    private static CassandraReloadableKeyValueServiceRuntimeConfig validate(
            CassandraReloadableKeyValueServiceRuntimeConfig instance) {
        instance.checkNonNegativeReplicationFactor();
        instance.checkPositiveNumberOfThriftHosts();
        instance.checkSharedGetRangesPoolGreaterThanOrEqualToConcurrentGetRangesThreadPool();
        return instance;
    }
}
