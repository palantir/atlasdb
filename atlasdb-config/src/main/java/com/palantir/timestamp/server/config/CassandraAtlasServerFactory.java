/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.timestamp.server.config;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.cleaner.Cleaner;
import com.palantir.atlasdb.cleaner.CleanupFollower;
import com.palantir.atlasdb.cleaner.DefaultCleanerBuilder;
import com.palantir.atlasdb.keyvalue.TableMappingService;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueService;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraTimestampBoundStore;
import com.palantir.atlasdb.keyvalue.impl.KVTableMappingService;
import com.palantir.atlasdb.keyvalue.impl.NamespaceMappingKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.TableRemappingKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.ValidatingQueryRewritingKeyValueService;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManager;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManagers;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;
import com.palantir.atlasdb.transaction.impl.SnapshotTransactionManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManagers;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.transaction.service.TransactionServices;
import com.palantir.lock.LockClient;
import com.palantir.lock.RemoteLockService;
import com.palantir.timestamp.PersistentTimestampService;
import com.palantir.timestamp.TimestampService;

public class CassandraAtlasServerFactory implements AtlasDbServerFactory {
    final CassandraKeyValueService rawKv;
    final KeyValueService kv;
    final SerializableTransactionManager txMgr;

    @Override
    public KeyValueService getKeyValueService() {
        return kv;
    }

    @Override
    public Supplier<TimestampService> getTimestampSupplier() {
        return new Supplier<TimestampService>() {
            @Override
            public TimestampService get() {
                return PersistentTimestampService.create(CassandraTimestampBoundStore.create(rawKv));
            }
        };
    }

    @Override
    public SerializableTransactionManager getTransactionManager() {
        return txMgr;
    }

    private CassandraAtlasServerFactory(CassandraKeyValueService rawKv,
                                        KeyValueService kv,
                                        SerializableTransactionManager txMgr) {
        this.rawKv = rawKv;
        this.kv = kv;
        this.txMgr = txMgr;
    }

    public static AtlasDbServerFactory create(CassandraKeyValueConfiguration config, Schema schema, TimestampService leadingTs, RemoteLockService leadingLock) {
        CassandraKeyValueService rawKv = createKv(config);
        KeyValueService keyValueService = createTableMappingKv(rawKv, leadingTs);

        schema.createTablesAndIndexes(keyValueService);
        SnapshotTransactionManager.createTables(keyValueService);

        TransactionService transactionService = TransactionServices.createTransactionService(keyValueService);
        LockClient client = LockClient.of("single node leveldb instance");
        ConflictDetectionManager conflictManager = ConflictDetectionManagers.createDefault(keyValueService);
        SweepStrategyManager sweepStrategyManager = SweepStrategyManagers.createDefault(keyValueService);

        CleanupFollower follower = CleanupFollower.create(schema);
        Cleaner cleaner = new DefaultCleanerBuilder(keyValueService, leadingLock, leadingTs, client, ImmutableList.of(follower), transactionService).buildCleaner();
        SerializableTransactionManager ret = new SerializableTransactionManager(
                keyValueService,
                leadingTs,
                client,
                leadingLock,
                transactionService,
                Suppliers.ofInstance(AtlasDbConstraintCheckingMode.FULL_CONSTRAINT_CHECKING_THROWS_EXCEPTIONS),
                conflictManager,
                sweepStrategyManager,
                cleaner);
        cleaner.start(ret);
        return new CassandraAtlasServerFactory(rawKv, keyValueService, ret);
    }

    private static KeyValueService createTableMappingKv(KeyValueService kv, final TimestampService ts) {
            TableMappingService mapper = getMapper(ts, kv);
            kv = NamespaceMappingKeyValueService.create(TableRemappingKeyValueService.create(kv, mapper));
            kv = ValidatingQueryRewritingKeyValueService.create(kv);
            return kv;
    }

    private static CassandraKeyValueService createKv(CassandraKeyValueConfiguration config) {
        return CassandraKeyValueService.create(ImmutableSet.copyOf(config.servers),
                config.port,
                config.poolSize,
                config.keyspace,
                config.isSsl,
                config.replicationFactor,
                config.mutationBatchCount,
                config.mutationBatchSizeBytes,
                config.fetchBatchCount,
                config.safetyDisabled,
                config.autoRefreshNodes);
    }

    private static TableMappingService getMapper(final TimestampService ts, KeyValueService kv) {
        return KVTableMappingService.create(kv, new Supplier<Long>() {
            @Override
            public Long get() {
                return ts.getFreshTimestamp();
            }
        });
    }

}
