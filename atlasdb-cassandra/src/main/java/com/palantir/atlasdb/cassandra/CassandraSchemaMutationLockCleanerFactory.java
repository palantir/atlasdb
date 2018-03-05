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

import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.auto.service.AutoService;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPool;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPoolImpl;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraSchemaLockCleaner;
import com.palantir.atlasdb.keyvalue.cassandra.SchemaMutationLockTables;
import com.palantir.atlasdb.keyvalue.cassandra.TracingQueryRunner;
import com.palantir.atlasdb.keyvalue.impl.TracingPrefsConfig;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.atlasdb.spi.SchemaMutationLockCleaner;
import com.palantir.atlasdb.spi.SchemaMutationLockCleanerFactory;

@AutoService(SchemaMutationLockCleanerFactory.class)
public final class CassandraSchemaMutationLockCleanerFactory implements SchemaMutationLockCleanerFactory {
    private static final Logger log = LoggerFactory.getLogger(CassandraSchemaMutationLockCleanerFactory.class);

    @Override
    public Supplier<SchemaMutationLockCleaner> getCleaner(KeyValueServiceConfig config) {
        CassandraKeyValueServiceConfig cassandraConfig = getCassandraKvsConfig(config);
        CassandraClientPool clientPool = CassandraClientPoolImpl.create(cassandraConfig);
        SchemaMutationLockTables lockTables = new SchemaMutationLockTables(clientPool, cassandraConfig);
        TracingQueryRunner tracingQueryRunner = new TracingQueryRunner(log, new TracingPrefsConfig());

        return () -> CassandraSchemaLockCleaner.create(cassandraConfig, clientPool, lockTables, tracingQueryRunner);
    }

    @Override
    public boolean supportsKVSType(String type) {
        return type.equalsIgnoreCase(CassandraKeyValueServiceConfig.TYPE);
    }

    private static CassandraKeyValueServiceConfig getCassandraKvsConfig(KeyValueServiceConfig config) {
        if (!config.type().equals(CassandraKeyValueServiceConfig.TYPE)) {
            throw new IllegalStateException(
                    String.format("KeyValueService must be of type %s, but yours is %s",
                            CassandraKeyValueServiceConfig.TYPE, config.type()));
        }
        return (CassandraKeyValueServiceConfig) config;
    }
}
