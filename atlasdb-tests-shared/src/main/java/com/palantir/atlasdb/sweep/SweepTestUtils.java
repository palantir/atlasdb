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
import com.palantir.atlasdb.keyvalue.api.watch.NoOpLockWatchManager;
import com.palantir.atlasdb.schema.SweepSchema;
import com.palantir.atlasdb.schema.TargetedSweepSchema;
import com.palantir.atlasdb.sweep.queue.MultiTableSweepQueueWriter;
import com.palantir.atlasdb.sweep.queue.TargetedSweeper;
import com.palantir.atlasdb.table.description.Schemas;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.impl.AbstractTransactionTestV2;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManager;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManagers;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManagers;
import com.palantir.atlasdb.transaction.impl.TransactionTables;
import com.palantir.atlasdb.transaction.knowledge.TransactionKnowledgeComponents;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.transaction.service.TransactionServices;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.LockService;
import com.palantir.lock.impl.LockServiceImpl;
import com.palantir.lock.v2.TimelockService;
import com.palantir.timelock.paxos.AbstractInMemoryTimelockExtension;
import com.palantir.timestamp.TimestampManagementService;
import java.time.Duration;
import java.util.function.Supplier;
import org.awaitility.Awaitility;

public final class SweepTestUtils {
    private SweepTestUtils() {}

    public static TransactionManager setupTxManager(
            KeyValueService kvs, AbstractInMemoryTimelockExtension abstractInMemoryTimelockExtension) {
        return setupTxManager(
                kvs,
                abstractInMemoryTimelockExtension.getLegacyTimelockService(),
                abstractInMemoryTimelockExtension.getTimestampManagementService(),
                SweepStrategyManagers.createDefault(kvs),
                TransactionServices.createRaw(kvs, abstractInMemoryTimelockExtension.getTimestampService(), false));
    }

    public static TransactionManager setupTxManager(
            KeyValueService kvs,
            TimelockService legacyTimelockService,
            TimestampManagementService tsmService,
            SweepStrategyManager ssm,
            TransactionService txService) {
        MetricsManager metricsManager = MetricsManagers.createForTests();
        LockService lockService = LockServiceImpl.create(
                LockServerOptions.builder().isStandaloneServer(false).build());
        Supplier<AtlasDbConstraintCheckingMode> constraints =
                () -> AtlasDbConstraintCheckingMode.NO_CONSTRAINT_CHECKING;
        ConflictDetectionManager cdm = ConflictDetectionManagers.createWithoutWarmingCache(kvs);
        Cleaner cleaner = new NoOpCleaner();
        MultiTableSweepQueueWriter writer = TargetedSweeper.createUninitializedForTest(() -> 1);
        TransactionKnowledgeComponents knowledge =
                TransactionKnowledgeComponents.createForTests(kvs, metricsManager.getTaggedRegistry());
        TransactionManager txManager = SerializableTransactionManager.createForTest(
                metricsManager,
                kvs,
                legacyTimelockService,
                tsmService,
                lockService,
                NoOpLockWatchManager.create(),
                txService,
                constraints,
                cdm,
                ssm,
                cleaner,
                AbstractTransactionTestV2.GET_RANGES_THREAD_POOL_SIZE,
                AbstractTransactionTestV2.DEFAULT_GET_RANGES_CONCURRENCY,
                writer,
                knowledge);
        setupTables(kvs);
        writer.initialize(txManager);
        return txManager;
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
