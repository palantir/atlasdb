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

import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueService;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraTimestampBoundStore;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraTimestampStoreInvalidator;
import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.atlasdb.versions.AtlasDbVersion;
import com.palantir.timestamp.PersistentTimestampService;
import com.palantir.timestamp.TimestampService;
import com.palantir.timestamp.TimestampStoreInvalidator;
import com.palantir.util.OptionalResolver;

@AutoService(AtlasDbFactory.class)
public class CassandraAtlasDbFactory implements AtlasDbFactory {
    @Override
    public KeyValueService createRawKeyValueService(
            KeyValueServiceConfig config,
            Optional<LeaderConfig> leaderConfig,
            Optional<String> namespace) {
        AtlasDbVersion.ensureVersionReported();
        CassandraKeyValueServiceConfig preprocessedConfig = preprocessKvsConfig(config, namespace);
        return createKv(preprocessedConfig, leaderConfig);
    }

    @VisibleForTesting
    static CassandraKeyValueServiceConfig preprocessKvsConfig(
            KeyValueServiceConfig config,
            Optional<String> namespace) {
        Preconditions.checkArgument(config instanceof CassandraKeyValueServiceConfig,
                "CassandraAtlasDbFactory expects a configuration of type"
                        + " CassandraKeyValueServiceConfig, found %s", config.getClass());
        CassandraKeyValueServiceConfig cassandraConfig = (CassandraKeyValueServiceConfig) config;

        String desiredKeyspace = OptionalResolver.resolve(namespace, cassandraConfig.keyspace());
        return CassandraKeyValueServiceConfigs.copyWithKeyspace(cassandraConfig, desiredKeyspace);
    }

    private static CassandraKeyValueService createKv(
            CassandraKeyValueServiceConfig config,
            Optional<LeaderConfig> leaderConfig) {
        return CassandraKeyValueService.create(
                CassandraKeyValueServiceConfigManager.createSimpleManager(config),
                leaderConfig);
    }

    @Override
    public TimestampService createTimestampService(KeyValueService rawKvs,
            Optional<TableReference> timestampTable) {
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
        return PersistentTimestampService.create(
                CassandraTimestampBoundStore.create((CassandraKeyValueService) rawKvs));
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
