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
package com.palantir.atlasdb.sweep;

import com.palantir.atlasdb.cleaner.NoOpCleaner;
import com.palantir.atlasdb.cleaner.api.Cleaner;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.persistentlock.KvsBackedPersistentLockService;
import com.palantir.atlasdb.persistentlock.NoOpPersistentLockService;
import com.palantir.atlasdb.persistentlock.PersistentLockService;
import com.palantir.atlasdb.schema.SweepSchema;
import com.palantir.atlasdb.schema.TargetedSweepSchema;
import com.palantir.atlasdb.sweep.queue.MultiTableSweepQueueWriter;
import com.palantir.atlasdb.sweep.queue.TargetedSweeper;
import com.palantir.atlasdb.table.description.Schemas;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.impl.AbstractTransactionTest;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManager;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManagers;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManagers;
import com.palantir.atlasdb.transaction.impl.TransactionTables;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.transaction.service.TransactionServices;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.LockService;
import com.palantir.lock.impl.LockServiceImpl;
import com.palantir.timestamp.InMemoryTimestampService;
import com.palantir.timestamp.TimestampManagementService;
import com.palantir.timestamp.TimestampService;
import java.time.Duration;
import java.util.function.Supplier;
import org.awaitility.Awaitility;

public final class SweepTestUtils {
    private SweepTestUtils() {}

    public static TransactionManager setupTxManager(KeyValueService kvs) {
        InMemoryTimestampService ts = new InMemoryTimestampService();
        return setupTxManager(
                kvs, ts, ts, SweepStrategyManagers.createDefault(kvs), TransactionServices.createRaw(kvs, ts, false));
    }

    public static TransactionManager setupTxManager(
            KeyValueService kvs,
            TimestampService tsService,
            TimestampManagementService tsmService,
            SweepStrategyManager ssm,
            TransactionService txService) {
        MetricsManager metricsManager = MetricsManagers.createForTests();
        LockClient lockClient = LockClient.of("sweep client");
        LockService lockService = LockServiceImpl.create(
                LockServerOptions.builder().isStandaloneServer(false).build());
        Supplier<AtlasDbConstraintCheckingMode> constraints =
                () -> AtlasDbConstraintCheckingMode.NO_CONSTRAINT_CHECKING;
        ConflictDetectionManager cdm = ConflictDetectionManagers.createWithoutWarmingCache(kvs);
        Cleaner cleaner = new NoOpCleaner();
        MultiTableSweepQueueWriter writer = TargetedSweeper.createUninitializedForTest(() -> 1);
        TransactionManager txManager = SerializableTransactionManager.createForTest(
                metricsManager,
                kvs,
                tsService,
                tsmService,
                lockClient,
                lockService,
                txService,
                constraints,
                cdm,
                ssm,
                cleaner,
                AbstractTransactionTest.GET_RANGES_THREAD_POOL_SIZE,
                AbstractTransactionTest.DEFAULT_GET_RANGES_CONCURRENCY,
                writer);
        setupTables(kvs);
        writer.initialize(txManager);
        return txManager;
    }

    public static PersistentLockService getPersistentLockService(KeyValueService kvs) {
        if (kvs.supportsCheckAndSet()) {
            return KvsBackedPersistentLockService.create(kvs);
        } else {
            return new NoOpPersistentLockService();
        }
    }

    static void setupTables(KeyValueService kvs) {
        tearDownTables(kvs);
        TransactionTables.createTables(kvs);
        Schemas.createTablesAndIndexes(SweepSchema.INSTANCE.getLatestSchema(), kvs);
    }

    private static void tearDownTables(KeyValueService kvs) {
        // do not truncate the targeted sweep table identifier tables
        Schemas.truncateTablesAndIndexes(TargetedSweepSchema.schemaWithoutTableIdentifierTables(), kvs);
        Awaitility.await().timeout(Duration.ofMinutes(5)).until(() -> {
            kvs.getAllTableNames().stream()
                    .filter(ref -> !TargetedSweepSchema.INSTANCE
                            .getLatestSchema()
                            .getAllTables()
                            .contains(ref))
                    .forEach(kvs::truncateTable);
            return true;
        });
    }
}
