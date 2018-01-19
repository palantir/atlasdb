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
public class CassandraReloadableKVSConfig extends AutoDelegate_CassandraKeyValueServiceConfig {
    private final CassandraKeyValueServiceConfig config;
    private final Supplier<Optional<KeyValueServiceRuntimeConfig>> runtimeConfig;

    public CassandraReloadableKVSConfig(CassandraKeyValueServiceConfig config,
            Supplier<Optional<KeyValueServiceRuntimeConfig>> runtimeConfig) {
        this.config = config;
        this.runtimeConfig = runtimeConfig;
    }

    @Override
    public CassandraKeyValueServiceConfig delegate() {
        return config;
    }

    @Override
    public int unresponsiveHostBackoffTimeSeconds() {
        return MoreObjects.firstNonNull(
                ifRuntimeConfigPresent(CassandraKeyValueServiceRuntimeConfig::unresponsiveHostBackoffTimeSeconds),
                config.unresponsiveHostBackoffTimeSeconds());
    }


    @Override
    public int mutationBatchCount() {
        return MoreObjects.firstNonNull(
                ifRuntimeConfigPresent(CassandraKeyValueServiceRuntimeConfig::mutationBatchCount),
                config.mutationBatchCount());
    }

    @Override
    public int mutationBatchSizeBytes() {
        return MoreObjects.firstNonNull(
                ifRuntimeConfigPresent(CassandraKeyValueServiceRuntimeConfig::mutationBatchSizeBytes),
                config.mutationBatchSizeBytes());
    }

    @Override
    public int fetchBatchCount() {
        return MoreObjects.firstNonNull(
                ifRuntimeConfigPresent(CassandraKeyValueServiceRuntimeConfig::fetchBatchCount),
                config.fetchBatchCount());
    }

    @Override
    public Integer sweepReadThreads() {
        return MoreObjects.firstNonNull(
                ifRuntimeConfigPresent(CassandraKeyValueServiceRuntimeConfig::sweepReadThreads),
                config.sweepReadThreads());
    }

    private <T> T ifRuntimeConfigPresent(Function<CassandraKeyValueServiceRuntimeConfig, T> function) {
        Optional<KeyValueServiceRuntimeConfig> runtimeConfigOptional = runtimeConfig.get();
        if (!runtimeConfigOptional.isPresent()) {
            return null;
        }
        CassandraKeyValueServiceRuntimeConfig runtimeConfig =
                (CassandraKeyValueServiceRuntimeConfig) runtimeConfigOptional.get();

        return function.apply(runtimeConfig);
    }
}
