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
package com.palantir.atlasdb.keyvalue.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.base.Suppliers;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cleaner.NoOpCleaner;
import com.palantir.atlasdb.cleaner.api.Cleaner;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.sweep.queue.MultiTableSweepQueueWriter;
import com.palantir.atlasdb.table.common.TableTasks;
import com.palantir.atlasdb.table.common.TableTasks.DiffStats;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.impl.AbstractTransactionTest;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManager;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManagers;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManagers;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.transaction.service.TransactionServices;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.impl.LockServiceImpl;
import com.palantir.timelock.paxos.InMemoryTimeLockRule;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class TableTasksTest {
    private MetricsManager metricsManager;
    private KeyValueService kvs;
    private LockServiceImpl lockService;
    private TransactionManager txManager;
    private TransactionService txService;

    @Rule
    public InMemoryTimeLockRule inMemoryTimeLockRule = new InMemoryTimeLockRule();

    @Before
    public void setup() {
        kvs = new InMemoryKeyValueService(true);

        LockClient lockClient = LockClient.of("sweep client");
        lockService = LockServiceImpl.create(
                LockServerOptions.builder().isStandaloneServer(false).build());
        txService = TransactionServices.createRaw(kvs, inMemoryTimeLockRule.getTimestampService(), false);
        Supplier<AtlasDbConstraintCheckingMode> constraints =
                Suppliers.ofInstance(AtlasDbConstraintCheckingMode.NO_CONSTRAINT_CHECKING);
        ConflictDetectionManager cdm = ConflictDetectionManagers.createWithoutWarmingCache(kvs);
        SweepStrategyManager ssm = SweepStrategyManagers.createDefault(kvs);
        Cleaner cleaner = new NoOpCleaner();
        metricsManager = MetricsManagers.createForTests();
        txManager = SerializableTransactionManager.createForTest(
                metricsManager,
                kvs,
                inMemoryTimeLockRule.getLegacyTimelockService(),
                inMemoryTimeLockRule.getTimestampManagementService(),
                lockService,
                inMemoryTimeLockRule.getLockWatchManager(),
                txService,
                constraints,
                cdm,
                ssm,
                cleaner,
                AbstractTransactionTest.GET_RANGES_THREAD_POOL_SIZE,
                AbstractTransactionTest.DEFAULT_GET_RANGES_CONCURRENCY,
                MultiTableSweepQueueWriter.NO_OP);
    }

    @After
    public void teardown() {
        lockService.close();
        kvs.close();
    }

    @Test
    public void testDiffTask() throws InterruptedException {
        TableReference table1 = TableReference.createWithEmptyNamespace("table1");
        TableReference table2 = TableReference.createWithEmptyNamespace("table2");
        Random rand = new Random();
        kvs.createTable(table1, AtlasDbConstants.GENERIC_TABLE_METADATA);
        kvs.createTable(table2, AtlasDbConstants.GENERIC_TABLE_METADATA);
        Multimap<Integer, Integer> keys1 = HashMultimap.create();
        Multimap<Integer, Integer> keys2 = HashMultimap.create();
        int key = 0;
        for (int col = 0; col < 256; col++) {
            int randomInt = rand.nextInt(3);
            if (randomInt >= 1) {
                keys1.put(key, col);
                kvs.put(
                        table1,
                        ImmutableMap.of(Cell.create(new byte[] {(byte) key}, new byte[] {(byte) col}), new byte[] {0}),
                        1);
            }
            if (randomInt <= 1) {
                keys2.put(key, col);
                kvs.put(
                        table2,
                        ImmutableMap.of(Cell.create(new byte[] {(byte) key}, new byte[] {(byte) col}), new byte[] {0}),
                        1);
            }
            if (rand.nextBoolean()) {
                key++;
            }
        }
        txService.update(1, 1);
        AtomicLong rowsOnlyInSource = new AtomicLong();
        AtomicLong rowsPartiallyInCommon = new AtomicLong();
        AtomicLong rowsCompletelyInCommon = new AtomicLong();
        AtomicLong rowsVisited = new AtomicLong();
        AtomicLong cellsOnlyInSource = new AtomicLong();
        AtomicLong cellsInCommon = new AtomicLong();
        DiffStats stats = new TableTasks.DiffStats(
                rowsOnlyInSource,
                rowsPartiallyInCommon,
                rowsCompletelyInCommon,
                rowsVisited,
                cellsOnlyInSource,
                cellsInCommon);
        TableTasks.diff(
                txManager,
                MoreExecutors.newDirectExecutorService(),
                table1,
                table2,
                10,
                1,
                stats,
                (transaction, partialDiff) -> partialDiff.forEachRemaining(_unused -> {}));
        long sourceOnlyCells = 0;
        long commonCells = 0;
        for (Map.Entry<Integer, Integer> cell : keys1.entries()) {
            if (keys2.containsEntry(cell.getKey(), cell.getValue())) {
                commonCells++;
            } else {
                sourceOnlyCells++;
            }
        }
        long disjointRows = 0;
        long partialRows = 0;
        long commonRows = 0;
        for (int k : keys1.keySet()) {
            if (Collections.disjoint(keys2.get(k), keys1.get(k))) {
                disjointRows++;
            } else if (keys2.get(k).containsAll(keys1.get(k))) {
                commonRows++;
            } else {
                partialRows++;
            }
        }

        assertThat(cellsInCommon.get()).isEqualTo(commonCells);
        assertThat(cellsOnlyInSource.get()).isEqualTo(sourceOnlyCells);
        assertThat(rowsOnlyInSource.get()).isEqualTo(disjointRows);
        assertThat(rowsCompletelyInCommon.get()).isEqualTo(commonRows);
        assertThat(rowsPartiallyInCommon.get()).isEqualTo(partialRows);
        assertThat(rowsVisited.get()).isEqualTo(keys1.keySet().size());
    }
}
