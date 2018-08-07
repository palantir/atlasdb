/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueServiceImpl;
import com.palantir.atlasdb.qos.QosClient;
import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.atlasdb.spi.KeyValueServiceRuntimeConfig;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.versions.AtlasDbVersion;
import com.palantir.util.OptionalResolver;

@AutoService(AtlasDbFactory.class)
public class CassandraAtlasDbFactory implements AtlasDbFactory {
    private static Logger log = LoggerFactory.getLogger(CassandraAtlasDbFactory.class);
    private CassandraKeyValueServiceRuntimeConfig latestValidRuntimeConfig =
            CassandraKeyValueServiceRuntimeConfig.getDefault();

    @Override
    public KeyValueService createRawKeyValueService(
            MetricsManager metricsManager,
            KeyValueServiceConfig config,
            Supplier<Optional<KeyValueServiceRuntimeConfig>> runtimeConfig,
            Optional<LeaderConfig> leaderConfig,
            Optional<String> namespace,
            LongSupplier freshTimestampSource,
            boolean initializeAsync,
            QosClient qosClient) {
        AtlasDbVersion.ensureVersionReported();
        CassandraKeyValueServiceConfig preprocessedConfig = preprocessKvsConfig(config, runtimeConfig, namespace);
        Supplier<CassandraKeyValueServiceRuntimeConfig> cassandraRuntimeConfig = preprocessKvsRuntimeConfig(
                runtimeConfig);
        return CassandraKeyValueServiceImpl.create(
                metricsManager,
                preprocessedConfig,
                cassandraRuntimeConfig,
                leaderConfig,
                CassandraMutationTimestampProviders.singleLongSupplierBacked(freshTimestampSource),
                initializeAsync,
                qosClient);
    }

    @VisibleForTesting
    static CassandraKeyValueServiceConfig preprocessKvsConfig(
            KeyValueServiceConfig config,
            Supplier<Optional<KeyValueServiceRuntimeConfig>> runtimeConfig,
            Optional<String> namespace) {
        Preconditions.checkArgument(config instanceof CassandraKeyValueServiceConfig,
                "Invalid KeyValueServiceConfig. Expected a KeyValueServiceConfig of type"
                        + " CassandraKeyValueServiceConfig, found %s.", config.getClass());
        CassandraKeyValueServiceConfig cassandraConfig = (CassandraKeyValueServiceConfig) config;

        String desiredKeyspace = OptionalResolver.resolve(namespace, cassandraConfig.keyspace());
        CassandraKeyValueServiceConfig configWithNamespace = CassandraKeyValueServiceConfigs
                .copyWithKeyspace(cassandraConfig, desiredKeyspace);

        return new CassandraReloadableKvsConfig(configWithNamespace, runtimeConfig);
    }

    @VisibleForTesting
    Supplier<CassandraKeyValueServiceRuntimeConfig> preprocessKvsRuntimeConfig(
            Supplier<Optional<KeyValueServiceRuntimeConfig>> runtimeConfig) {
        return () -> {
            Optional<KeyValueServiceRuntimeConfig> configOptional = runtimeConfig.get();

            return configOptional.map(config -> {
                if (!(config instanceof CassandraKeyValueServiceRuntimeConfig)) {
                    log.error("Invalid KeyValueServiceRuntimeConfig. Expected a KeyValueServiceRuntimeConfig of type"
                                    + " CassandraKeyValueServiceRuntimeConfig, found %s."
                                    + " Using latest valid CassandraKeyValueServiceRuntimeConfig.",
                            config.getClass());
                    return latestValidRuntimeConfig;
                }

                latestValidRuntimeConfig = (CassandraKeyValueServiceRuntimeConfig) config;
                return latestValidRuntimeConfig;
            }).orElse(CassandraKeyValueServiceRuntimeConfig.getDefault());
        };
    }

    @Override
    public String getType() {
        return CassandraKeyValueServiceConfig.TYPE;
    }
}
