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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.internalschema.TransactionSchemaManager;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.SweepStatsKeyValueService;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy;
import com.palantir.atlasdb.schema.generated.SweepTableFactory;
import com.palantir.atlasdb.sweep.metrics.LegacySweepMetrics;
import com.palantir.atlasdb.sweep.metrics.SweepOutcomeMetrics;
import com.palantir.atlasdb.sweep.priority.NextTableToSweepProvider;
import com.palantir.atlasdb.sweep.priority.SweepPriority;
import com.palantir.atlasdb.sweep.priority.SweepPriorityOverrideConfig;
import com.palantir.atlasdb.sweep.priority.SweepPriorityStoreImpl;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManagers;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.transaction.service.TransactionServices;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.common.base.ClosableIterator;
import com.palantir.lock.SingleLockService;
import com.palantir.timelock.paxos.InMemoryTimeLockRule;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BooleanSupplier;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public abstract class AbstractBackgroundSweeperIntegrationTest {
    static final TableReference TABLE_1 = TableReference.createFromFullyQualifiedName("foo.bar");
    private static final TableReference TABLE_2 = TableReference.createFromFullyQualifiedName("qwe.rty");
    private static final TableReference TABLE_3 = TableReference.createFromFullyQualifiedName("baz.qux");

    private final MetricsManager metricsManager = MetricsManagers.createForTests();
    protected KeyValueService kvs;
    protected TransactionManager txManager;
    private BackgroundSweepThread backgroundSweeper;
    private SweepBatchConfig sweepBatchConfig = ImmutableSweepBatchConfig.builder()
            .deleteBatchSize(8)
            .candidateBatchSize(15)
            .maxCellTsPairsToExamine(1000)
            .build();
    protected TransactionService txService;
    protected TransactionSchemaManager txSchemaManager = mock(TransactionSchemaManager.class);
    SpecificTableSweeper specificTableSweeper;
    AdjustableSweepBatchConfigSource sweepBatchConfigSource;
    PeriodicTrueSupplier skipCellVersion = new PeriodicTrueSupplier();

    @ClassRule
    public static InMemoryTimeLockRule services = new InMemoryTimeLockRule();

    @Before
    public void setup() {
        kvs = SweepStatsKeyValueService.create(
                getKeyValueService(),
                services.getTimestampService(),
                () -> AtlasDbConstants.DEFAULT_SWEEP_WRITE_THRESHOLD,
                () -> AtlasDbConstants.DEFAULT_SWEEP_WRITE_SIZE_THRESHOLD,
                () -> true);
        SweepStrategyManager ssm = SweepStrategyManagers.createDefault(kvs);
        txService = TransactionServices.createV1TransactionService(kvs);
        txManager = SweepTestUtils.setupTxManager(
                kvs,
                services.getLegacyTimelockService(),
                services.getTimestampManagementService(),
                ssm,
                txService
        );
        CellsSweeper cellsSweeper = new CellsSweeper(txManager, kvs, ImmutableList.of());
        SweepTaskRunner sweepRunner = new SweepTaskRunner(
                kvs,
                AbstractBackgroundSweeperIntegrationTest::getTimestamp,
                AbstractBackgroundSweeperIntegrationTest::getTimestamp,
                txService,
                cellsSweeper,
                null);
        LegacySweepMetrics sweepMetrics = new LegacySweepMetrics(metricsManager.getRegistry());
        specificTableSweeper = SpecificTableSweeper.create(
                txManager,
                kvs,
                sweepRunner,
                SweepTableFactory.of(),
                new NoOpBackgroundSweeperPerformanceLogger(),
                sweepMetrics,
                false);

        sweepBatchConfigSource = AdjustableSweepBatchConfigSource.create(metricsManager, () -> sweepBatchConfig);

        backgroundSweeper = new BackgroundSweepThread(
                txManager.getLockService(),
                NextTableToSweepProvider.create(
                        kvs, txManager.getLockService(), specificTableSweeper.getSweepPriorityStore()),
                sweepBatchConfigSource,
                () -> true, // sweepEnabled
                () -> 10L, // sweepPauseMillis
                SweepPriorityOverrideConfig::defaultConfig,
                specificTableSweeper,
                SweepOutcomeMetrics.registerLegacy(metricsManager),
                new CountDownLatch(1),
                0);
        when(txSchemaManager.getTransactionsSchemaVersion(anyLong())).thenReturn(1);
    }

    @After
    public void closeTransactionManager() {
        txManager.close();
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
        try (SingleLockService sweepLocks = backgroundSweeper.createSweepLocks()) {
            for (int i = 0; i < 50; ++i) {
                backgroundSweeper.checkConfigAndRunSweep(sweepLocks);
            }
        }
        verifyTableSwept(TABLE_1, 75, true);
        verifyTableSwept(TABLE_2, 58, false);
        List<SweepPriority> priorities =
                txManager.runTaskReadOnly(tx -> SweepPriorityStoreImpl.create(kvs, SweepTableFactory.of(), false)
                        .loadNewPriorities(tx));
        assertThat(priorities.stream().anyMatch(p -> p.tableRef().equals(TABLE_1)))
                .isTrue();
        assertThat(priorities.stream().anyMatch(p -> p.tableRef().equals(TABLE_2)))
                .isTrue();
    }

    protected abstract KeyValueService getKeyValueService();

    void verifyTableSwept(TableReference tableRef, int expectedCells, boolean conservative) {
        try (ClosableIterator<RowResult<Set<Long>>> iter =
                kvs.getRangeOfTimestamps(tableRef, RangeRequest.all(), Long.MAX_VALUE)) {
            int numCells = 0;
            while (iter.hasNext()) {
                RowResult<Set<Long>> rr = iter.next();
                numCells += rr.getColumns().size();
                if (conservative) {
                    assertThat(rr.getColumns().values().stream().allMatch(s -> s.size() == 2 && s.contains(-1L)))
                            .describedAs(
                                    "Expected a sentinel and exactly one other value!", tableRef.getQualifiedName())
                            .isTrue();
                } else {
                    assertThat(rr.getColumns().values().stream().allMatch(s -> s.size() <= 1 && !s.contains(-1L)))
                            .describedAs("Expected at most one, non-sentinel, value!", tableRef.getQualifiedName())
                            .isTrue();
                }
            }
            assertThat(numCells).isEqualTo(expectedCells);
        }
    }

    protected void createTable(TableReference tableReference, SweepStrategy sweepStrategy) {
        kvs.createTable(
                tableReference,
                new TableDefinition() {
                    {
                        rowName();
                        rowComponent("row", ValueType.BLOB);
                        columns();
                        column("col", "c", ValueType.BLOB);
                        conflictHandler(ConflictHandler.IGNORE_ALL);
                        sweepStrategy(sweepStrategy);
                    }
                }.toTableMetadata().persistToBytes());
    }

    /**
     * Magic number alert! This will add 75 entries of which 17 will be deletes
     */
    void putManyCells(TableReference tableRef, long startTs, long commitTs) {
        Map<Cell, byte[]> cells = new HashMap<>();
        for (int i = 0; i < 50; ++i) {
            cells.put(
                    Cell.create(Ints.toByteArray(i), "c".getBytes(StandardCharsets.UTF_8)),
                    (i % 3 == 0) ? new byte[] {} : Ints.toByteArray(123456 + i));
            if (i % 2 == 0) {
                cells.put(
                        Cell.create(Ints.toByteArray(i), "d".getBytes(StandardCharsets.UTF_8)),
                        Ints.toByteArray(9876543 - i));
            }
        }
        kvs.put(tableRef, cells, startTs);
        txService.putUnlessExists(startTs, commitTs);
    }

    private static long getTimestamp() {
        return 150L;
    }

    protected static final class PeriodicTrueSupplier implements BooleanSupplier {
        private int truePeriod = 1;
        private long count = 0;
        private boolean deterministic = true;

        private PeriodicTrueSupplier() {}

        public void setPeriod(int period) {
            truePeriod = period;
        }

        public void makeNonDeterministic() {
            deterministic = false;
        }

        @Override
        public boolean getAsBoolean() {
            if (deterministic) {
                ++count;
                return count % truePeriod == 0;
            }
            return ThreadLocalRandom.current().nextInt(100) == 0;
        }
    }
}
