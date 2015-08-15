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
import com.palantir.atlasdb.keyvalue.impl.ValidatingQueryRewritingKeyValueService;
import com.palantir.atlasdb.keyvalue.leveldb.impl.LevelDbBoundStore;
import com.palantir.atlasdb.keyvalue.leveldb.impl.LevelDbKeyValueService;
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
import com.palantir.common.base.Throwables;
import com.palantir.lock.LockClient;
import com.palantir.lock.RemoteLockService;
import com.palantir.timestamp.PersistentTimestampService;
import com.palantir.timestamp.TimestampService;

public class LevelDbAtlasServerFactory {
    public static AtlasDbServerState create(String dataDir, Schema schema, TimestampService leaderTs, RemoteLockService leaderLock) {
        final LevelDbKeyValueService rawKv = createKv(dataDir);
        KeyValueService keyValueService = createTableMappingKv(rawKv, leaderTs);

        SnapshotTransactionManager.createTables(keyValueService);

        TransactionService transactionService = TransactionServices.createTransactionService(keyValueService);
        LockClient client = LockClient.of("single node leveldb instance");
        ConflictDetectionManager conflictManager = ConflictDetectionManagers.createDefault(keyValueService);
        SweepStrategyManager sweepStrategyManager = SweepStrategyManagers.createDefault(keyValueService);

        CleanupFollower follower = CleanupFollower.create(schema);
        Cleaner cleaner = new DefaultCleanerBuilder(keyValueService, leaderLock, leaderTs, client, ImmutableList.of(follower), transactionService).buildCleaner();
        SerializableTransactionManager ret = new SerializableTransactionManager(
                keyValueService,
                leaderTs,
                client,
                leaderLock,
                transactionService,
                Suppliers.ofInstance(AtlasDbConstraintCheckingMode.FULL_CONSTRAINT_CHECKING_THROWS_EXCEPTIONS),
                conflictManager,
                sweepStrategyManager,
                cleaner);
        cleaner.start(ret);

        Supplier<TimestampService> tsSupplier = new Supplier<TimestampService>() {
            @Override
            public TimestampService get() {
                return PersistentTimestampService.create(LevelDbBoundStore.create(rawKv));
            }
        };
        return new AtlasDbServerState(keyValueService, tsSupplier, ret);
    }

    private static KeyValueService createTableMappingKv(KeyValueService kv, final TimestampService ts) {
            TableMappingService mapper = getMapper(ts, kv);
            kv = NamespaceMappingKeyValueService.create(TableRemappingKeyValueService.create(kv, mapper));
            kv = ValidatingQueryRewritingKeyValueService.create(kv);
            return kv;
    }

    private static LevelDbKeyValueService createKv(String dataDir) {
        try {
            return LevelDbKeyValueService.create(new File(dataDir));
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
