/**
 * Copyright 2015 Palantir Technologies
 * <p>
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://opensource.org/licenses/BSD-3-Clause
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.cassandra;

import org.apache.thrift.TException;

import com.google.auto.service.AutoService;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueService;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraTimestampBoundStore;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraTimestampStoreInvalidator;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraVerifier;
import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.atlasdb.versions.AtlasDbVersion;
import com.palantir.timestamp.DebugLogger;
import com.palantir.timestamp.PersistentTimestampService;
import com.palantir.timestamp.TimestampService;
import com.palantir.timestamp.TimestampStoreInvalidator;

@AutoService(AtlasDbFactory.class)
public class CassandraAtlasDbFactory implements AtlasDbFactory {
    @Override
    public KeyValueService createRawKeyValueService(
            KeyValueServiceConfig config,
            Optional<LeaderConfig> leaderConfig) {
        AtlasDbVersion.ensureVersionReported();
        Preconditions.checkArgument(config instanceof CassandraKeyValueServiceConfig,
                "CassandraAtlasDbFactory expects a configuration of type"
                        + " CassandraKeyValueServiceConfig, found %s", config.getClass());
        return createKv((CassandraKeyValueServiceConfig) config, leaderConfig);
    }

    @Override
    public Supplier<KeyValueService> createRawKeyValueServiceSupplier(
            KeyValueServiceConfig config,
            Optional<LeaderConfig> leaderConfig) {
        AtlasDbVersion.ensureVersionReported();
        Preconditions.checkArgument(config instanceof CassandraKeyValueServiceConfig,
                "CassandraAtlasDbFactory expects a configuration of type"
                        + " CassandraKeyValueServiceConfig, found %s", config.getClass());
        CassandraKeyValueServiceConfig cassandraKeyValueServiceConfig = (CassandraKeyValueServiceConfig) config;
        if (cassandraKeyValueServiceConfig.startWithoutCassandraUp()) {
            return new CassandraKeyValueServiceSupplier(cassandraKeyValueServiceConfig, leaderConfig);
        } else {
            return Suppliers.memoize(() -> createRawKeyValueService(config, leaderConfig));
        }
    }

    @Override
    public Supplier<TimestampService> createTimestampServiceSupplier(Supplier<KeyValueService> kvsSupplier) {
        return () -> (kvsSupplier.get() != null) ? createTimestampService(kvsSupplier.get()) : null;
    }

    @Override
    public Supplier<TimestampStoreInvalidator> createTimestampStoreInvalidatorSupplier(
            Supplier<KeyValueService> kvsSupplier) {
        return () -> (kvsSupplier.get() != null) ? createTimestampStoreInvalidator(kvsSupplier.get()) : null;
    }


    private static CassandraKeyValueService createKv(
            CassandraKeyValueServiceConfig config,
            Optional<LeaderConfig> leaderConfig) {
        return CassandraKeyValueService.create(
                CassandraKeyValueServiceConfigManager.createSimpleManager(config),
                leaderConfig);
    }

    @Override
    public TimestampService createTimestampService(KeyValueService rawKvs) {
        DebugLogger.logger.info("Creating timestamp service on thread {}. This should only happen once.",
                Thread.currentThread().getName());

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

    static class CassandraKeyValueServiceSupplier implements Supplier<KeyValueService> {
        private final CassandraKeyValueServiceConfig config;
        private final Optional<LeaderConfig> leaderConfig;
        private CassandraKeyValueService kvs;

        CassandraKeyValueServiceSupplier(CassandraKeyValueServiceConfig config, Optional<LeaderConfig> leaderConfig) {
            this.config = config;
            this.leaderConfig = leaderConfig;
        }

        @Override
        public KeyValueService get() {
            if (kvs != null) {
                return kvs;
            }
            try {
                kvs = createKv(config, leaderConfig);
            } catch (RuntimeException ex) {
                if (ex.getCause() instanceof TException
                        && ex.getCause().getMessage().equals(CassandraVerifier.KEYSPACE_CREATION_FAILED_MESSAGE)) {
                    kvs = null;
                }
            }
            return kvs;
        }
    }
}
