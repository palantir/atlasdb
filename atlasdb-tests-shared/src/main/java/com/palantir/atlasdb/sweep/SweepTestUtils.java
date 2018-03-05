/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.sweep;

import org.awaitility.Awaitility;
import org.awaitility.Duration;

import com.google.common.base.Supplier;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cleaner.Cleaner;
import com.palantir.atlasdb.cleaner.NoOpCleaner;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.persistentlock.KvsBackedPersistentLockService;
import com.palantir.atlasdb.persistentlock.NoOpPersistentLockService;
import com.palantir.atlasdb.persistentlock.PersistentLockService;
import com.palantir.atlasdb.schema.SweepSchema;
import com.palantir.atlasdb.table.description.Schemas;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionManager;
import com.palantir.atlasdb.transaction.impl.AbstractTransactionTest;
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
import com.palantir.lock.LockService;
import com.palantir.lock.impl.LockServiceImpl;
import com.palantir.timestamp.InMemoryTimestampService;
import com.palantir.timestamp.TimestampService;

public final class SweepTestUtils {
    private SweepTestUtils() {}

    public static LockAwareTransactionManager setupTxManager(KeyValueService kvs) {
        return setupTxManager(
                kvs,
                new InMemoryTimestampService(),
                SweepStrategyManagers.createDefault(kvs),
                TransactionServices.createTransactionService(kvs));
    }

    public static LockAwareTransactionManager setupTxManager(KeyValueService kvs,
            TimestampService tsService,
            SweepStrategyManager ssm,
            TransactionService txService) {
        LockClient lockClient = LockClient.of("sweep client");
        LockService lockService = LockServiceImpl.create(
                LockServerOptions.builder().isStandaloneServer(false).build());
        Supplier<AtlasDbConstraintCheckingMode> constraints = () ->
                AtlasDbConstraintCheckingMode.NO_CONSTRAINT_CHECKING;
        ConflictDetectionManager cdm = ConflictDetectionManagers.createWithoutWarmingCache(kvs);
        Cleaner cleaner = new NoOpCleaner();
        LockAwareTransactionManager txManager = SerializableTransactionManager.createForTest(
                kvs, tsService, lockClient, lockService, txService, constraints, cdm, ssm, cleaner,
                AbstractTransactionTest.GET_RANGES_THREAD_POOL_SIZE,
                AbstractTransactionTest.DEFAULT_GET_RANGES_CONCURRENCY,
                () -> AtlasDbConstants.DEFAULT_TIMESTAMP_CACHE_SIZE);
        setupTables(kvs);
        return txManager;
    }

    public static PersistentLockService getPersistentLockService(KeyValueService kvs) {
        if (kvs.supportsCheckAndSet()) {
            return KvsBackedPersistentLockService.create(kvs, () -> null);
        } else {
            return new NoOpPersistentLockService();
        }
    }

    private static void setupTables(KeyValueService kvs) {
        tearDownTables(kvs);
        TransactionTables.createTables(kvs);
        Schemas.createTablesAndIndexes(SweepSchema.INSTANCE.getLatestSchema(), kvs);
    }

    private static void tearDownTables(KeyValueService kvs) {
        Awaitility.await()
                .timeout(Duration.FIVE_MINUTES)
                .until(() -> {
                    kvs.getAllTableNames().forEach(kvs::dropTable);
                    return true;
                });
        TransactionTables.deleteTables(kvs);
        Schemas.deleteTablesAndIndexes(SweepSchema.INSTANCE.getLatestSchema(), kvs);
    }
}
