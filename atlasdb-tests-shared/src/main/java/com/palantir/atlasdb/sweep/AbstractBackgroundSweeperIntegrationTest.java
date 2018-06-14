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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.SweepStatsKeyValueService;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy;
import com.palantir.atlasdb.schema.generated.SweepTableFactory;
import com.palantir.atlasdb.sweep.metrics.SweepMetricsManager;
import com.palantir.atlasdb.sweep.priority.NextTableToSweepProvider;
import com.palantir.atlasdb.sweep.priority.SweepPriority;
import com.palantir.atlasdb.sweep.priority.SweepPriorityOverrideConfig;
import com.palantir.atlasdb.sweep.priority.SweepPriorityStoreImpl;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManagers;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.transaction.service.TransactionServices;
import com.palantir.common.base.ClosableIterator;
import com.palantir.lock.SingleLockService;
import com.palantir.timestamp.InMemoryTimestampService;
import com.palantir.timestamp.TimestampService;

public abstract class AbstractBackgroundSweeperIntegrationTest {

    protected static final TableReference TABLE_1 = TableReference.createFromFullyQualifiedName("foo.bar");
    private static final TableReference TABLE_2 = TableReference.createFromFullyQualifiedName("qwe.rty");
    private static final TableReference TABLE_3 = TableReference.createFromFullyQualifiedName("baz.qux");

    protected KeyValueService kvs;
    protected LockAwareTransactionManager txManager;
    protected final AtomicLong sweepTimestamp = new AtomicLong();
    private BackgroundSweepThread backgroundSweeper;
    private SweepBatchConfig sweepBatchConfig = ImmutableSweepBatchConfig.builder()
            .deleteBatchSize(8)
            .candidateBatchSize(15)
            .maxCellTsPairsToExamine(1000)
            .build();
    protected TransactionService txService;
    protected SpecificTableSweeper specificTableSweeper;
    protected AdjustableSweepBatchConfigSource sweepBatchConfigSource;

    @Before
    public void setup() {
        TimestampService tsService = new InMemoryTimestampService();
        kvs = SweepStatsKeyValueService.create(getKeyValueService(), tsService,
                () -> AtlasDbConstants.DEFAULT_SWEEP_WRITE_THRESHOLD,
                () -> AtlasDbConstants.DEFAULT_SWEEP_WRITE_SIZE_THRESHOLD
        );
        SweepStrategyManager ssm = SweepStrategyManagers.createDefault(kvs);
        txService = TransactionServices.createTransactionService(kvs);
        txManager = SweepTestUtils.setupTxManager(kvs, tsService, ssm, txService);
        LongSupplier tsSupplier = sweepTimestamp::get;
        PersistentLockManager persistentLockManager = new PersistentLockManager(
                SweepTestUtils.getPersistentLockService(kvs),
                AtlasDbConstants.DEFAULT_SWEEP_PERSISTENT_LOCK_WAIT_MILLIS);
        CellsSweeper cellsSweeper = new CellsSweeper(txManager, kvs, persistentLockManager, ImmutableList.of());
        SweepTaskRunner sweepRunner = new SweepTaskRunner(kvs, tsSupplier, tsSupplier, txService, ssm, cellsSweeper);
        SweepMetricsManager sweepMetricsManager = new SweepMetricsManager();
        specificTableSweeper = SpecificTableSweeper.create(
                txManager,
                kvs,
                sweepRunner,
                SweepTableFactory.of(),
                new NoOpBackgroundSweeperPerformanceLogger(),
                sweepMetricsManager,
                false);

        sweepBatchConfigSource = AdjustableSweepBatchConfigSource.create(() -> sweepBatchConfig);

        backgroundSweeper = new BackgroundSweepThread(
                txManager.getLockService(),
                NextTableToSweepProvider.create(kvs,
                        txManager.getLockService(),
                        specificTableSweeper.getSweepPriorityStore()),
                sweepBatchConfigSource,
                () -> true, // sweepEnabled
                () -> 10L, // sweepPauseMillis
                SweepPriorityOverrideConfig::defaultConfig,
                specificTableSweeper,
                new SweepOutcomeMetrics(),
                new CountDownLatch(1),
                0);
    }

    @Test
    public void smokeTest() throws Exception {
        createTable(TABLE_1, SweepStrategy.CONSERVATIVE);
        createTable(TABLE_2, SweepStrategy.THOROUGH);
        createTable(TABLE_3, SweepStrategy.NOTHING);
        putManyCells(TABLE_1, 100, 110);
        putManyCells(TABLE_1, 103, 113);
        putManyCells(TABLE_1, 105, 115);
        putManyCells(TABLE_2, 101, 111);
        putManyCells(TABLE_2, 104, 114);
        putManyCells(TABLE_3, 120, 130);
        sweepTimestamp.set(150);
        try (SingleLockService sweepLocks = backgroundSweeper.createSweepLocks()) {
            for (int i = 0; i < 50; ++i) {
                backgroundSweeper.checkConfigAndRunSweep(sweepLocks);
            }
        }
        verifyTableSwept(TABLE_1, 75, true);
        verifyTableSwept(TABLE_2, 58, false);
        List<SweepPriority> priorities = txManager.runTaskReadOnly(
                tx -> SweepPriorityStoreImpl.create(kvs, SweepTableFactory.of(), false).loadNewPriorities(tx));
        Assert.assertTrue(priorities.stream().anyMatch(p -> p.tableRef().equals(TABLE_1)));
        Assert.assertTrue(priorities.stream().anyMatch(p -> p.tableRef().equals(TABLE_2)));
    }

    protected abstract KeyValueService getKeyValueService();

    protected void verifyTableSwept(TableReference tableRef, int expectedCells, boolean conservative) {
        try (ClosableIterator<RowResult<Set<Long>>> iter =
                kvs.getRangeOfTimestamps(tableRef, RangeRequest.all(), Long.MAX_VALUE)) {
            int numCells = 0;
            while (iter.hasNext()) {
                RowResult<Set<Long>> rr = iter.next();
                numCells += rr.getColumns().size();
                Assert.assertTrue(String.format("Found unswept values in %s!", tableRef.getQualifiedName()),
                        rr.getColumns().values().stream().allMatch(
                        s -> s.size() == 1 || (conservative && s.size() == 2 && s.contains(-1L))));
            }
            Assert.assertEquals(expectedCells, numCells);
        }
    }

    protected void createTable(TableReference tableReference, SweepStrategy sweepStrategy) {
        kvs.createTable(tableReference,
                new TableDefinition() {
                    {
                        rowName();
                        rowComponent("row", ValueType.BLOB);
                        columns();
                        column("col", "c", ValueType.BLOB);
                        conflictHandler(ConflictHandler.IGNORE_ALL);
                        sweepStrategy(sweepStrategy);
                    }
                }.toTableMetadata().persistToBytes()
        );
    }

    protected void putManyCells(TableReference tableRef, long startTs, long commitTs) {
        Map<Cell, byte[]> cells = Maps.newHashMap();
        for (int i = 0; i < 50; ++i) {
            cells.put(Cell.create(Ints.toByteArray(i), "c".getBytes()),
                    (i % 3 == 0) ? new byte[] {} : Ints.toByteArray(123456 + i));
            if (i % 2 == 0) {
                cells.put(Cell.create(Ints.toByteArray(i), "d".getBytes()), Ints.toByteArray(9876543 - i));
            }
        }
        kvs.put(tableRef, cells, startTs);
        txService.putUnlessExists(startTs, commitTs);
    }
}
