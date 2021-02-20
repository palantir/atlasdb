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
import com.palantir.atlasdb.spi.KeyValueServiceRuntimeConfig;
import com.palantir.refreshable.Refreshable;
import java.util.Optional;
import java.util.function.Supplier;

public class CassandraReloadableKvsConfig extends ForwardingCassandraKeyValueServiceConfig {

    private final CassandraKeyValueServiceConfig config;
    private final Supplier<Optional<CassandraKeyValueServiceRuntimeConfig>> runtimeConfigSupplier;

    public CassandraReloadableKvsConfig(
            CassandraKeyValueServiceConfig config,
            Refreshable<Optional<KeyValueServiceRuntimeConfig>> runtimeConfigRefreshable) {
        this.config = config;
        this.runtimeConfigSupplier = runtimeConfigRefreshable.map(runtimeConfig -> {
            Optional<CassandraKeyValueServiceRuntimeConfig> cassandraRuntimeConfig =
                    runtimeConfig.map(CassandraKeyValueServiceRuntimeConfig.class::cast);

            checkArgument(
                    servers(config, cassandraRuntimeConfig).numberOfThriftHosts() > 0,
                    "'servers' must have at least one defined host");

            return cassandraRuntimeConfig;
        });
    }

    @Override
    public CassandraKeyValueServiceConfig delegate() {
        return config;
    }

    @Override
    public CassandraServersConfig servers() {
        return servers(config, runtimeConfigSupplier.get());
    }

    private static CassandraServersConfig servers(
            CassandraKeyValueServiceConfig config, Optional<CassandraKeyValueServiceRuntimeConfig> runtimeConfig) {
        if (config.servers().numberOfThriftHosts() > 0) {
            return config.servers();
        }

        return runtimeConfig.map(CassandraKeyValueServiceRuntimeConfig::servers).orElseGet(config::servers);
    }

    @Override
    public int unresponsiveHostBackoffTimeSeconds() {
        return runtimeConfigSupplier
                .get()
                .map(CassandraKeyValueServiceRuntimeConfig::unresponsiveHostBackoffTimeSeconds)
                .orElseGet(config::unresponsiveHostBackoffTimeSeconds);
    }

    @Override
    public int mutationBatchCount() {
        return runtimeConfigSupplier
                .get()
                .map(CassandraKeyValueServiceRuntimeConfig::mutationBatchCount)
                .orElseGet(config::mutationBatchCount);
    }

    @Override
    public int mutationBatchSizeBytes() {
        return runtimeConfigSupplier
                .get()
                .map(CassandraKeyValueServiceRuntimeConfig::mutationBatchSizeBytes)
                .orElseGet(config::mutationBatchSizeBytes);
    }

    @Override
    public int fetchBatchCount() {
        return runtimeConfigSupplier
                .get()
                .map(CassandraKeyValueServiceRuntimeConfig::fetchBatchCount)
                .orElseGet(config::fetchBatchCount);
    }

    @Override
    public Integer sweepReadThreads() {
        return runtimeConfigSupplier
                .get()
                .map(CassandraKeyValueServiceRuntimeConfig::sweepReadThreads)
                .orElseGet(config::sweepReadThreads);
    }

    @Override
    public int concurrentGetRangesThreadPoolSize() {
        if (config.servers().numberOfThriftHosts() > 0) {
            return config.concurrentGetRangesThreadPoolSize();
        }

        // Use the initial number of thrift hosts as a best guess
        return runtimeConfigSupplier
                .get()
                .map(_runtime -> poolSize() * servers().numberOfThriftHosts())
                .orElseGet(config::concurrentGetRangesThreadPoolSize);
    }
}
