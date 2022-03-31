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
import com.palantir.atlasdb.spi.DerivedConcurrencyConfig;
import com.palantir.logsafe.SafeArg;
import com.palantir.refreshable.Refreshable;

public class CassandraReloadableKeyValueServiceRuntimeConfig extends ForwardingCassandraKeyValueServiceRuntimeConfig
        implements DerivedConcurrencyConfig {

    private final CassandraKeyValueServiceConfig installConfig;
    private final CassandraKeyValueServiceRuntimeConfig runtimeConfig;

    private CassandraReloadableKeyValueServiceRuntimeConfig(
            CassandraKeyValueServiceConfig installConfig, CassandraKeyValueServiceRuntimeConfig runtimeConfig) {
        this.installConfig = installConfig;
        this.runtimeConfig = runtimeConfig;
    }

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
    public int replicationFactor() {
        return installConfig.replicationFactor().orElseGet(runtimeConfig::replicationFactor);
    }

    @Override
    public int concurrentGetRangesThreadPoolSize() {
        return installConfig
                .concurrentGetRangesThreadPoolSize()
                .orElseGet(() -> installConfig.poolSize() * servers().numberOfThriftHosts());
    }

    @Override
    public int defaultGetRangesConcurrency() {
        return installConfig
                .defaultGetRangesConcurrency()
                .orElseGet(() -> Math.min(8, concurrentGetRangesThreadPoolSize() / 2));
    }

    private static CassandraReloadableKeyValueServiceRuntimeConfig validate(
            CassandraReloadableKeyValueServiceRuntimeConfig instance) {
        checkArgument(instance.servers().numberOfThriftHosts() > 0, "'servers' must have at least one defined host");

        checkArgument(instance.replicationFactor() >= 0, "'replicationFactor' must be set to a non-negative number");

        instance.installConfig
                .sharedResourcesConfig()
                .ifPresent(config -> checkArgument(
                        config.sharedGetRangesPoolSize() >= instance.concurrentGetRangesThreadPoolSize(),
                        "If set, shared get ranges pool size must not be less than individual pool size.",
                        SafeArg.of("shared", config.sharedGetRangesPoolSize()),
                        SafeArg.of("individual", instance.concurrentGetRangesThreadPoolSize())));
        return instance;
    }
}
