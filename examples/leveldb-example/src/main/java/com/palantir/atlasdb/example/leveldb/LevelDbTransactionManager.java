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
package com.palantir.atlasdb.example.leveldb;

import java.io.File;
import java.io.IOException;

import org.iq80.leveldb.DBException;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.cleaner.Cleaner;
import com.palantir.atlasdb.cleaner.CleanupFollower;
import com.palantir.atlasdb.cleaner.DefaultCleanerBuilder;
import com.palantir.atlasdb.keyvalue.TableMappingService;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.KVTableMappingService;
import com.palantir.atlasdb.keyvalue.impl.NamespaceMappingKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.TableRemappingKeyValueService;
import com.palantir.atlasdb.keyvalue.leveldb.impl.LevelDbBoundStore;
import com.palantir.atlasdb.keyvalue.leveldb.impl.LevelDbKeyValueService;
import com.palantir.atlasdb.schema.AtlasSchema;
import com.palantir.atlasdb.schema.Namespace;
import com.palantir.atlasdb.schema.SchemaReference;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionManager;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManager;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManagers;
import com.palantir.atlasdb.transaction.impl.SnapshotTransactionManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManagers;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.transaction.service.TransactionServices;
import com.palantir.common.base.Throwables;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.RemoteLockService;
import com.palantir.lock.client.LockRefreshingLockService;
import com.palantir.lock.impl.LockServiceImpl;
import com.palantir.timestamp.PersistentTimestampService;
import com.palantir.timestamp.TimestampService;

public class LevelDbTransactionManager {

    public static LockAwareTransactionManager createTransactionManager(Schema schema) {
        return createTransactionManagerInternal(schema, null);
    }

    public static LockAwareTransactionManager createTransactionManager(SchemaReference schemaRef) {
        return createTransactionManagerInternal(schemaRef.getSchema(), schemaRef.getNamespace());
    }

    public static LockAwareTransactionManager createTransactionManager(AtlasSchema schema) {
        return createTransactionManagerInternal(schema.getLatestSchema(), schema.getNamespace());
    }

    private static SnapshotTransactionManager createTransactionManagerInternal(Schema schema, Namespace namespace) {
        LevelDbKeyValueService levelKv = createKv();
        TimestampService ts = PersistentTimestampService.create(LevelDbBoundStore.create(levelKv));
        KeyValueService keyValueService = createTableMappingKv(levelKv, ts);

        schema.createTablesAndIndexes(keyValueService);
        SnapshotTransactionManager.createTables(keyValueService);
        if (namespace != null && namespace != Namespace.EMPTY_NAMESPACE) {
            TableRemappingKeyValueService mappingKv = TableRemappingKeyValueService.create(keyValueService, getMapper(ts, keyValueService));
            KeyValueService namespaceKvs = NamespaceMappingKeyValueService.create(mappingKv);
            schema.createTablesAndIndexes(namespaceKvs);
        }

        TransactionService transactionService = TransactionServices.createTransactionService(keyValueService);
        RemoteLockService lock = LockRefreshingLockService.create(LockServiceImpl.create(new LockServerOptions() {
            private static final long serialVersionUID = 1L;

            @Override
            public boolean isStandaloneServer() {
                return false;
            }
        }));
        LockClient client = LockClient.of("single node leveldb instance");
        ConflictDetectionManager conflictManager = ConflictDetectionManagers.createDefault(keyValueService);
        SweepStrategyManager sweepStrategyManager = SweepStrategyManagers.createDefault(keyValueService);

        CleanupFollower follower = CleanupFollower.create(schema);
        Cleaner cleaner = new DefaultCleanerBuilder(keyValueService, lock, ts, client, ImmutableList.of(follower), transactionService).buildCleaner();
        SnapshotTransactionManager ret = new SnapshotTransactionManager(
                keyValueService,
                ts,
                client,
                lock,
                transactionService,
                Suppliers.ofInstance(AtlasDbConstraintCheckingMode.FULL_CONSTRAINT_CHECKING_THROWS_EXCEPTIONS),
                conflictManager,
                sweepStrategyManager,
                cleaner);
        cleaner.start(ret);
        return ret;
    }

    private static KeyValueService createTableMappingKv(KeyValueService kv, final TimestampService ts) {
            TableMappingService mapper = getMapper(ts, kv);
            return NamespaceMappingKeyValueService.create(TableRemappingKeyValueService.create(kv, mapper));
    }

    private static LevelDbKeyValueService createKv() {
        try {
            return LevelDbKeyValueService.create(new File("testdb"));
        } catch (DBException | IOException e) {
            throw Throwables.throwUncheckedException(e);
        }
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
