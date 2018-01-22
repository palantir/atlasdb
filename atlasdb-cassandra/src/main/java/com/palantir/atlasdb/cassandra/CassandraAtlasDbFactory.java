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
import java.util.function.Supplier;

import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueService;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueServiceImpl;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraTimestampBoundStore;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraTimestampStoreInvalidator;
import com.palantir.atlasdb.qos.QosClient;
import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.atlasdb.spi.KeyValueServiceRuntimeConfig;
import com.palantir.atlasdb.versions.AtlasDbVersion;
import com.palantir.timestamp.PersistentTimestampServiceImpl;
import com.palantir.timestamp.TimestampService;
import com.palantir.timestamp.TimestampStoreInvalidator;
import com.palantir.util.OptionalResolver;

@AutoService(AtlasDbFactory.class)
public class CassandraAtlasDbFactory implements AtlasDbFactory {
    @Override
    public KeyValueService createRawKeyValueService(
            KeyValueServiceConfig config,
            Supplier<Optional<KeyValueServiceRuntimeConfig>> runtimeConfig,
            Optional<LeaderConfig> leaderConfig,
            Optional<String> namespace,
            boolean initializeAsync,
            QosClient qosClient) {
        AtlasDbVersion.ensureVersionReported();
        CassandraKeyValueServiceConfig preprocessedConfig = preprocessKvsConfig(config, runtimeConfig, namespace);
        Supplier<CassandraKeyValueServiceRuntimeConfig> cassandraRuntimeConfig = preprocessKvsRuntimeConfig(
                runtimeConfig);
        return CassandraKeyValueServiceImpl.create(
                preprocessedConfig,
                cassandraRuntimeConfig,
                leaderConfig,
                initializeAsync,
                qosClient);
    }

    @VisibleForTesting
    static CassandraKeyValueServiceConfig preprocessKvsConfig(
            KeyValueServiceConfig config,
            Supplier<Optional<KeyValueServiceRuntimeConfig>> runtimeConfig,
            Optional<String> namespace) {
        CassandraKeyValueServiceConfig cassandraConfig = checkAndCast(config, CassandraKeyValueServiceConfig.class);

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
            if (configOptional.isPresent()) {
                return checkAndCast(configOptional.get(), CassandraKeyValueServiceRuntimeConfig.class);
            }

            return CassandraKeyValueServiceRuntimeConfig.getDefault();
        };
    }

    @SuppressWarnings("unchecked")
    private static <T> T checkAndCast(Object obj, Class<T> clazz) {
        Preconditions.checkArgument(clazz.isInstance(obj),
                "CassandraAtlasDbFactory expects an instance of type %s, found %s", clazz.getClass(), obj.getClass());
        return (T) obj;
    }

    @Override
    public TimestampService createTimestampService(
            KeyValueService rawKvs,
            Optional<TableReference> timestampTable,
            boolean initializeAsync) {

        Preconditions.checkArgument(!timestampTable.isPresent()
                        || timestampTable.get().equals(AtlasDbConstants.TIMESTAMP_TABLE),
                "***ERROR:This can cause severe data corruption.***\nUnexpected timestamp table found: "
                        + timestampTable.map(TableReference::getQualifiedName).orElse("unknown table")
                        + "\nThis can happen if you configure the timelock server to use Cassandra KVS for timestamp"
                        + " persistence, which is unsupported.\nWe recommend using the default paxos timestamp"
                        + " persistence. However, if you are need to persist the timestamp service state in the"
                        + " database, please specify a valid DbKvs config in the timestampBoundPersister block."
                        + "\nNote that if the service has already been running, you will have to migrate the timestamp"
                        + " table to Postgres/Oracle and rename it to %s.",
                AtlasDbConstants.TIMELOCK_TIMESTAMP_TABLE);

        AtlasDbVersion.ensureVersionReported();
        Preconditions.checkArgument(rawKvs instanceof CassandraKeyValueService,
                "TimestampService must be created from an instance of"
                + " CassandraKeyValueService, found %s", rawKvs.getClass());
        return PersistentTimestampServiceImpl.create(
                CassandraTimestampBoundStore.create((CassandraKeyValueService) rawKvs, initializeAsync),
                initializeAsync);
    }

    @Override
    public String getType() {
        return CassandraKeyValueServiceConfig.TYPE;
    }

    @Override
    public TimestampStoreInvalidator createTimestampStoreInvalidator(KeyValueService rawKvs) {
        return CassandraTimestampStoreInvalidator.create(rawKvs);
    }
}
