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
import com.palantir.atlasdb.spi.DerivedSnapshotConfig;
import com.palantir.logsafe.SafeArg;
import com.palantir.refreshable.Refreshable;
import org.immutables.value.Value;

@Value.Immutable
public abstract class CassandraReloadableKeyValueServiceRuntimeConfig
        extends ForwardingCassandraKeyValueServiceRuntimeConfig implements DerivedSnapshotConfig {

    @Value.Parameter
    abstract CassandraKeyValueServiceConfig installConfig();

    @Value.Parameter
    abstract CassandraKeyValueServiceRuntimeConfig runtimeConfig();

    static Refreshable<CassandraReloadableKeyValueServiceRuntimeConfig> fromConfigs(
            CassandraKeyValueServiceConfig installConfig,
            Refreshable<CassandraKeyValueServiceRuntimeConfig> runtimeConfigRefreshable) {
        return runtimeConfigRefreshable.map(runtimeConfig ->
                ImmutableCassandraReloadableKeyValueServiceRuntimeConfig.of(installConfig, runtimeConfig));
    }

    @Override
    @Value.Derived
    public CassandraKeyValueServiceRuntimeConfig delegate() {
        return runtimeConfig();
    }

    @Override
    @Value.Derived
    public CassandraServersConfig servers() {
        if (installConfig().servers().numberOfThriftHosts() > 0) {
            return installConfig().servers();
        }
        return runtimeConfig().servers();
    }

    @Override
    @Value.Derived
    public int concurrentGetRangesThreadPoolSize() {
        if (installConfig().concurrentGetRangesThreadPoolSize() > 0) {
            return installConfig().concurrentGetRangesThreadPoolSize();
        }
        return installConfig().poolSize() * servers().numberOfThriftHosts();
    }

    @Override
    @Value.Derived
    public int defaultGetRangesConcurrency() {
        if (installConfig().defaultGetRangesConcurrency() > 0) {
            return installConfig().defaultGetRangesConcurrency();
        }
        return Math.min(8, concurrentGetRangesThreadPoolSize() / 2);
    }

    @Value.Check
    void checkPositiveNumberOfThriftHosts() {
        checkArgument(servers().numberOfThriftHosts() > 0, "'servers' must have at least one defined host");
    }

    @Value.Check
    void checkSharedGetRangesPoolGreaterThanOrEqualToConcurrentGetRangesThreadPool() {
        installConfig()
                .sharedResourcesConfig()
                .ifPresent(config -> checkArgument(
                        config.sharedGetRangesPoolSize() >= concurrentGetRangesThreadPoolSize(),
                        "If set, shared get ranges pool size must not be less than individual pool size.",
                        SafeArg.of("shared", config.sharedGetRangesPoolSize()),
                        SafeArg.of("individual", concurrentGetRangesThreadPoolSize())));
    }
}
