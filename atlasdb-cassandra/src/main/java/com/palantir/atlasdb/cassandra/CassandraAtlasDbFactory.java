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

import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs.CassandraServersConfig;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs.ThriftHostsExtractingVisitor;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig.Builder;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueService;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueServiceImpl;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraTimestampBoundStore;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraTimestampStoreInvalidator;
import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.atlasdb.spi.KeyValueServiceRuntimeConfig;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.versions.AtlasDbVersion;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.refreshable.Refreshable;
import com.palantir.timestamp.ManagedTimestampService;
import com.palantir.timestamp.PersistentTimestampServiceImpl;
import com.palantir.timestamp.TimestampStoreInvalidator;
import com.palantir.util.OptionalResolver;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoService(AtlasDbFactory.class)
public class CassandraAtlasDbFactory implements AtlasDbFactory {
    private static Logger log = LoggerFactory.getLogger(CassandraAtlasDbFactory.class);
    private CassandraKeyValueServiceRuntimeConfig latestValidRuntimeConfig =
            CassandraKeyValueServiceRuntimeConfig.getDefault();

    @Override
    public KeyValueService createRawKeyValueService(
            MetricsManager metricsManager,
            KeyValueServiceConfig config,
            Refreshable<Optional<KeyValueServiceRuntimeConfig>> runtimeConfig,
            Optional<LeaderConfig> unused,
            Optional<String> namespace,
            LongSupplier freshTimestampSource,
            boolean initializeAsync) {
        AtlasDbVersion.ensureVersionReported();
        CassandraKeyValueServiceConfig preprocessedConfig = preprocessKvsConfig(config, runtimeConfig, namespace);
        Supplier<CassandraKeyValueServiceRuntimeConfig> cassandraRuntimeConfig =
                preprocessKvsRuntimeConfig(runtimeConfig);
        return CassandraKeyValueServiceImpl.create(
                metricsManager,
                preprocessedConfig,
                cassandraRuntimeConfig,
                CassandraMutationTimestampProviders.singleLongSupplierBacked(freshTimestampSource),
                initializeAsync);
    }

    @VisibleForTesting
    static CassandraKeyValueServiceConfig preprocessKvsConfig(
            KeyValueServiceConfig config,
            Refreshable<Optional<KeyValueServiceRuntimeConfig>> runtimeConfigRefreshable,
            Optional<String> namespace) {
        Preconditions.checkArgument(
                config instanceof CassandraKeyValueServiceConfig,
                "Invalid KeyValueServiceConfig. Expected a KeyValueServiceConfig of type"
                        + " CassandraKeyValueServiceConfig",
                SafeArg.of("foundClass", config.getClass()));
        CassandraKeyValueServiceConfig cassandraConfig = (CassandraKeyValueServiceConfig) config;

        Refreshable<CassandraKeyValueServiceConfig> mergedConfig = runtimeConfigRefreshable.map(runtimeConfig -> {
            Builder builder = ImmutableCassandraKeyValueServiceConfig.builder().from(cassandraConfig);

            builder.keyspace(OptionalResolver.resolve(namespace, cassandraConfig.keyspace()));

            // only get from runtime config if install config wasn't defined (for backcompat)
            if (cassandraConfig.servers().numberOfThriftHosts() <= 0) {
                CassandraServersConfig servers = chooseConfig(
                        runtimeConfig, CassandraKeyValueServiceRuntimeConfig::servers, cassandraConfig.servers());
                Preconditions.checkState(
                        !servers.accept(new ThriftHostsExtractingVisitor()).isEmpty(),
                        "'servers' must have at least one defined host");
                builder.servers(servers);
            }

            // only get from runtime config if install config wasn't defined (for backcompat)
            if (config.concurrentGetRangesThreadPoolSize() <= 0) {
                builder.concurrentGetRangesThreadPoolSize(chooseConfig(
                        runtimeConfig,
                        CassandraKeyValueServiceRuntimeConfig::concurrentGetRangesThreadPoolSize,
                        cassandraConfig.concurrentGetRangesThreadPoolSize()));
            }

            builder.poolSize(chooseConfig(
                    runtimeConfig, CassandraKeyValueServiceRuntimeConfig::poolSize, cassandraConfig.poolSize()));

            builder.unresponsiveHostBackoffTimeSeconds(chooseConfig(
                    runtimeConfig,
                    CassandraKeyValueServiceRuntimeConfig::unresponsiveHostBackoffTimeSeconds,
                    cassandraConfig.unresponsiveHostBackoffTimeSeconds()));

            builder.mutationBatchCount(chooseConfig(
                    runtimeConfig,
                    CassandraKeyValueServiceRuntimeConfig::mutationBatchCount,
                    cassandraConfig.mutationBatchCount()));

            builder.mutationBatchSizeBytes(chooseConfig(
                    runtimeConfig,
                    CassandraKeyValueServiceRuntimeConfig::mutationBatchSizeBytes,
                    cassandraConfig.mutationBatchSizeBytes()));

            builder.fetchBatchCount(chooseConfig(
                    runtimeConfig,
                    CassandraKeyValueServiceRuntimeConfig::fetchBatchCount,
                    cassandraConfig.fetchBatchCount()));

            builder.sweepReadThreads(chooseConfig(
                    runtimeConfig,
                    CassandraKeyValueServiceRuntimeConfig::sweepReadThreads,
                    cassandraConfig.sweepReadThreads()));

            return builder.build();
        });
        return new DelegatingCassandraKeyValueServiceConfig(mergedConfig);
    }

    private static <T> T chooseConfig(
            Optional<KeyValueServiceRuntimeConfig> runtimeConfigOptional,
            Function<CassandraKeyValueServiceRuntimeConfig, T> runtimeConfig,
            T installConfig) {
        return MoreObjects.firstNonNull(unwrapRuntimeConfig(runtimeConfigOptional, runtimeConfig), installConfig);
    }

    private static <T> T unwrapRuntimeConfig(
            Optional<KeyValueServiceRuntimeConfig> runtimeConfigOptional,
            Function<CassandraKeyValueServiceRuntimeConfig, T> function) {
        if (!runtimeConfigOptional.isPresent()) {
            return null;
        }
        CassandraKeyValueServiceRuntimeConfig ckvsRuntimeConfig =
                (CassandraKeyValueServiceRuntimeConfig) runtimeConfigOptional.get();

        return function.apply(ckvsRuntimeConfig);
    }

    @VisibleForTesting
    Supplier<CassandraKeyValueServiceRuntimeConfig> preprocessKvsRuntimeConfig(
            Supplier<Optional<KeyValueServiceRuntimeConfig>> runtimeConfig) {
        return () -> {
            Optional<KeyValueServiceRuntimeConfig> configOptional = runtimeConfig.get();

            return configOptional
                    .map(config -> {
                        if (!(config instanceof CassandraKeyValueServiceRuntimeConfig)) {
                            log.error(
                                    "Invalid KeyValueServiceRuntimeConfig. Expected a KeyValueServiceRuntimeConfig of"
                                            + " type CassandraKeyValueServiceRuntimeConfig, found {}. Using latest valid"
                                            + " CassandraKeyValueServiceRuntimeConfig.",
                                    config.getClass());
                            return latestValidRuntimeConfig;
                        }

                        latestValidRuntimeConfig = (CassandraKeyValueServiceRuntimeConfig) config;
                        return latestValidRuntimeConfig;
                    })
                    .orElseGet(CassandraKeyValueServiceRuntimeConfig::getDefault);
        };
    }

    @Override
    public ManagedTimestampService createManagedTimestampService(
            KeyValueService rawKvs, Optional<TableReference> tableReferenceOverride, boolean initializeAsync) {
        Preconditions.checkArgument(
                tableReferenceOverride
                        .map(AtlasDbConstants.TIMESTAMP_TABLE::equals)
                        .orElse(true),
                "***ERROR:This can cause severe data corruption.***\nUnexpected timestamp table override found: "
                        + tableReferenceOverride
                        + "\nThis can happen if you configure the timelock server to use Cassandra KVS for timestamp"
                        + " persistence, which is unsupported.\nWe recommend using the default paxos timestamp"
                        + " persistence. However, if you are need to persist the timestamp service state in the"
                        + " database, please specify a valid DbKvs config in the timestampBoundPersister block."
                        + "\nNote that if the service has already been running, you will have to migrate the timestamp"
                        + " table to Postgres/Oracle: please contact support. DO NOT TRY TO FIX THIS YOURSELF.");

        AtlasDbVersion.ensureVersionReported();
        Preconditions.checkArgument(
                rawKvs instanceof CassandraKeyValueService,
                "TimestampService must be created from an instance of" + " CassandraKeyValueService",
                SafeArg.of("foundClass", rawKvs.getClass()));
        return PersistentTimestampServiceImpl.create(
                CassandraTimestampBoundStore.create((CassandraKeyValueService) rawKvs, initializeAsync),
                initializeAsync);
    }

    @Override
    public String getType() {
        return CassandraKeyValueServiceConfig.TYPE;
    }

    @Override
    public TimestampStoreInvalidator createTimestampStoreInvalidator(
            KeyValueService rawKvs, Optional<TableReference> unused) {
        return CassandraTimestampStoreInvalidator.create(rawKvs);
    }
}
