/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.cassandra;

import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.base.MoreObjects;
import com.palantir.atlasdb.spi.KeyValueServiceRuntimeConfig;
import com.palantir.processors.AutoDelegate;

@AutoDelegate(typeToExtend = CassandraKeyValueServiceConfig.class)
public class CassandraReloadableKvsConfig extends AutoDelegate_CassandraKeyValueServiceConfig {
    private final CassandraKeyValueServiceConfig config;
    private final Supplier<Optional<KeyValueServiceRuntimeConfig>> runtimeConfigSupplier;

    public CassandraReloadableKvsConfig(CassandraKeyValueServiceConfig config,
            Supplier<Optional<KeyValueServiceRuntimeConfig>> runtimeConfig) {
        this.config = config;
        this.runtimeConfigSupplier = runtimeConfig;
    }

    @Override
    public CassandraKeyValueServiceConfig delegate() {
        return config;
    }

    @Override
    public int unresponsiveHostBackoffTimeSeconds() {
        return chooseConfig(CassandraKeyValueServiceRuntimeConfig::unresponsiveHostBackoffTimeSeconds,
                config.unresponsiveHostBackoffTimeSeconds());
    }


    @Override
    public int mutationBatchCount() {
        return chooseConfig(CassandraKeyValueServiceRuntimeConfig::mutationBatchCount,
                config.mutationBatchCount());
    }

    @Override
    public int mutationBatchSizeBytes() {
        return chooseConfig(CassandraKeyValueServiceRuntimeConfig::mutationBatchSizeBytes,
                config.mutationBatchSizeBytes());
    }

    @Override
    public int fetchBatchCount() {
        return chooseConfig(CassandraKeyValueServiceRuntimeConfig::fetchBatchCount,
                config.fetchBatchCount());
    }

    @Override
    public Integer sweepReadThreads() {
        return chooseConfig(CassandraKeyValueServiceRuntimeConfig::sweepReadThreads,
                config.sweepReadThreads());
    }

    private <T> T chooseConfig(Function<CassandraKeyValueServiceRuntimeConfig, T> runtimeConfig, T installConfig) {
        return MoreObjects.firstNonNull(
                unwrapRuntimeConfig(runtimeConfig),
                installConfig);
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
