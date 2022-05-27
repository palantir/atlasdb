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
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueService;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueServiceImpl;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraTimestampBoundStore;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraTimestampStoreInvalidator;
import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.atlasdb.spi.DerivedSnapshotConfig;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.atlasdb.spi.KeyValueServiceRuntimeConfig;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.versions.AtlasDbVersion;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.refreshable.Refreshable;
import com.palantir.timestamp.ManagedTimestampService;
import com.palantir.timestamp.PersistentTimestampServiceImpl;
import com.palantir.timestamp.TimestampStoreInvalidator;
import com.palantir.util.OptionalResolver;
import java.util.Optional;
import java.util.function.LongSupplier;

@AutoService(AtlasDbFactory.class)
public class CassandraAtlasDbFactory implements AtlasDbFactory {
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
        CassandraKeyValueServiceConfigs configs =
                CassandraKeyValueServiceConfigs.fromKeyValueServiceConfigsOrThrow(config, runtimeConfig);

        CassandraKeyValueServiceConfigs configsWithNamespace = configs.copyWithKeyspace(
                OptionalResolver.resolve(namespace, configs.installConfig().namespace()));

        return CassandraKeyValueServiceImpl.create(
                metricsManager,
                configsWithNamespace.installConfig(),
                configsWithNamespace.runtimeConfig(),
                CassandraMutationTimestampProviders.singleLongSupplierBacked(freshTimestampSource),
                initializeAsync);
    }

    @Override
    public DerivedSnapshotConfig createDerivedSnapshotConfig(
            KeyValueServiceConfig config, Optional<KeyValueServiceRuntimeConfig> runtimeConfigSnapshot) {
        CassandraKeyValueServiceConfigs configs = CassandraKeyValueServiceConfigs.fromKeyValueServiceConfigsOrThrow(
                config, Refreshable.only(runtimeConfigSnapshot));

        CassandraReloadableKeyValueServiceRuntimeConfig runtimeConfig =
                CassandraReloadableKeyValueServiceRuntimeConfig.fromConfigs(
                                configs.installConfig(), configs.runtimeConfig())
                        .get();
        return DerivedSnapshotConfig.builder()
                .concurrentGetRangesThreadPoolSize(runtimeConfig.concurrentGetRangesThreadPoolSize())
                .defaultGetRangesConcurrencyOverride(runtimeConfig.defaultGetRangesConcurrency())
                .build();
    }

    @Override
    public ManagedTimestampService createManagedTimestampService(
            KeyValueService rawKvs, Optional<TableReference> tableReferenceOverride, boolean initializeAsync) {
        Preconditions.checkArgument(
                tableReferenceOverride
                        .map(AtlasDbConstants.TIMESTAMP_TABLE::equals)
                        .orElse(true),
                "***ERROR:This can cause severe data corruption.***\nUnexpected timestamp table override was found."
                        + "\nThis can happen if you configure the timelock server to use Cassandra KVS for timestamp"
                        + " persistence, which is unsupported.\nWe recommend using the default paxos timestamp"
                        + " persistence. However, if you are need to persist the timestamp service state in the"
                        + " database, please specify a valid DbKvs config in the timestampBoundPersister block."
                        + "\nNote that if the service has already been running, you will have to migrate the timestamp"
                        + " table to Postgres/Oracle: please contact support. DO NOT TRY TO FIX THIS YOURSELF.",
                UnsafeArg.of("tableReferenceOverride", tableReferenceOverride));

        AtlasDbVersion.ensureVersionReported();
        Preconditions.checkArgument(
                rawKvs instanceof CassandraKeyValueService,
                "TimestampService must be created from an instance of CassandraKeyValueService, but found a different"
                        + " key value service",
                SafeArg.of("rawKvs", rawKvs.getClass()));
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
