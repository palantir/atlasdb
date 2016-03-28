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
package com.palantir.atlasdb.memory;

import com.google.auto.service.AutoService;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.cleaner.Cleaner;
import com.palantir.atlasdb.cleaner.CleanupFollower;
import com.palantir.atlasdb.cleaner.DefaultCleanerBuilder;
import com.palantir.atlasdb.cleaner.api.OnCleanupTask;
import com.palantir.atlasdb.keyvalue.TableMappingService;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.NamespaceMappingKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.StaticTableMappingService;
import com.palantir.atlasdb.keyvalue.impl.TableRemappingKeyValueService;
import com.palantir.atlasdb.schema.AtlasSchema;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.table.description.Schemas;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManager;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManagers;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManagers;
import com.palantir.atlasdb.transaction.impl.TransactionTables;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.transaction.service.TransactionServices;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.RemoteLockService;
import com.palantir.lock.client.LockRefreshingLockService;
import com.palantir.lock.impl.LockServiceImpl;
import com.palantir.timestamp.InMemoryTimestampService;
import com.palantir.timestamp.TimestampService;

/**
 * This is the easiest way to try out AtlasDB with your schema.  It runs entirely in memory but has
 * all the features of a full cluster including {@link OnCleanupTask}s.
 * <p>
 * This method creates all the tables in the pass {@link Schema} and provides Snapshot Isolation
 * (SI) on all of the transactions it creates.
 */
@AutoService(AtlasDbFactory.class)
public class InMemoryAtlasDbFactory implements AtlasDbFactory {

    @Override
    public String getType() {
        return "memory";
    }

    @Override
    public InMemoryKeyValueService createRawKeyValueService(KeyValueServiceConfig config) {
        return new InMemoryKeyValueService(false);
    }

    @Override
    public TimestampService createTimestampService(KeyValueService rawKvs) {
        return new InMemoryTimestampService();
    }

    public static SerializableTransactionManager createInMemoryTransactionManager(AtlasSchema schema) {
        return createInMemoryTransactionManagerInternal(schema.getLatestSchema(), schema.getNamespace());
    }

    private static SerializableTransactionManager createInMemoryTransactionManagerInternal(Schema schema, Namespace namespace) {
        TimestampService ts = new InMemoryTimestampService();
        KeyValueService keyValueService = createTableMappingKv(ts);

        Schemas.createTablesAndIndexes(schema, keyValueService);
        TransactionTables.createTables(keyValueService);

        TransactionService transactionService = TransactionServices.createTransactionService(keyValueService);
        RemoteLockService lock = LockRefreshingLockService.create(LockServiceImpl.create(new LockServerOptions() {
            private static final long serialVersionUID = 1L;

            @Override
            public boolean isStandaloneServer() {
                return false;
            }
        }));
        LockClient client = LockClient.of("in memory atlasdb instance");
        ConflictDetectionManager conflictManager = ConflictDetectionManagers.createDefault(keyValueService);
        SweepStrategyManager sweepStrategyManager = SweepStrategyManagers.createDefault(keyValueService);

        CleanupFollower follower = CleanupFollower.create(schema);
        Cleaner cleaner = new DefaultCleanerBuilder(keyValueService, lock, ts, client, ImmutableList.of(follower), transactionService).buildCleaner();
        SerializableTransactionManager ret = new SerializableTransactionManager(
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

    private static KeyValueService createTableMappingKv(final TimestampService ts) {
        KeyValueService kv = new InMemoryKeyValueService(false);
        TableMappingService mapper = getMapper(kv);
        return NamespaceMappingKeyValueService.create(TableRemappingKeyValueService.create(kv, mapper));
    }

    private static TableMappingService getMapper(KeyValueService kv) {
        return StaticTableMappingService.create(kv);
    }
}
