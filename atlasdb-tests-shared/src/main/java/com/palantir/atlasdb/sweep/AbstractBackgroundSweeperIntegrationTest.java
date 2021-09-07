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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
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
import com.palantir.common.streams.KeyedStream;
import com.palantir.lock.SingleLockService;
import com.palantir.timestamp.InMemoryTimestampService;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.After;
import org.junit.Before;
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
    private TransactionService txService;
    SpecificTableSweeper specificTableSweeper;
    AdjustableSweepBatchConfigSource sweepBatchConfigSource;
    DeterministicTrueSupplier skipCellVersion = new DeterministicTrueSupplier();

    @Before
    public void setup() {
        InMemoryTimestampService tsService = new InMemoryTimestampService();
        kvs = SweepStatsKeyValueService.create(
                getKeyValueService(),
                tsService,
                () -> AtlasDbConstants.DEFAULT_SWEEP_WRITE_THRESHOLD,
                () -> AtlasDbConstants.DEFAULT_SWEEP_WRITE_SIZE_THRESHOLD,
                () -> true);
        SweepStrategyManager ssm = SweepStrategyManagers.createDefault(kvs);
        txService = TransactionServices.createV1TransactionService(kvs);
        txManager = SweepTestUtils.setupTxManager(kvs, tsService, tsService, ssm, txService);
        PersistentLockManager persistentLockManager = new PersistentLockManager(
                metricsManager,
                SweepTestUtils.getPersistentLockService(kvs),
                AtlasDbConstants.DEFAULT_SWEEP_PERSISTENT_LOCK_WAIT_MILLIS);
        CellsSweeper cellsSweeper = new CellsSweeper(txManager, kvs, persistentLockManager, ImmutableList.of());
        SweepTaskRunner sweepRunner = new SweepTaskRunner(
                kvs,
                AbstractBackgroundSweeperIntegrationTest::getTimestamp,
                AbstractBackgroundSweeperIntegrationTest::getTimestamp,
                txService,
                ssm,
                cellsSweeper,
                null,
                skipCellVersion);
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

    @Test
    public void previouslyConservativeSweepsEverythingWhenNothingIsSkipped() {
        skipCellVersion.setPeriod(Integer.MAX_VALUE);
        createTable(TABLE_1, SweepStrategy.THOROUGH);
        putManyCells(TABLE_1, 100, 101);
        putManyCells(TABLE_1, 103, 104);
        putManyCells(TABLE_1, 107, 109);

        SweeperService sweeperService = new SweeperServiceImpl(specificTableSweeper, sweepBatchConfigSource);
        sweeperService.sweepPreviouslyConservativeNowThoroughTable(
                TABLE_1.getQualifiedName(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        verifyTableSwept(TABLE_1, 58, false);
    }

    @Test
    public void previouslyConservativeErasesExistingSentinelsInThoroughTable() {
        skipCellVersion.setPeriod(Integer.MAX_VALUE);
        createTable(TABLE_1, SweepStrategy.THOROUGH);

        Map<Cell, byte[]> sentinelWrites = IntStream.range(0, 100)
                .mapToObj(PtBytes::toBytes)
                .map(bytes -> Cell.create(bytes, bytes))
                .collect(Collectors.toMap(cell -> cell, _ignore -> PtBytes.EMPTY_BYTE_ARRAY));
        kvs.put(TABLE_1, sentinelWrites, Value.INVALID_VALUE_TIMESTAMP);
        Map<Cell, Long> readMap = KeyedStream.stream(sentinelWrites)
                .map(_ignore -> Long.MAX_VALUE)
                .collectToMap();
        assertThat(kvs.get(TABLE_1, readMap)).hasSize(100);

        SweeperService sweeperService = new SweeperServiceImpl(specificTableSweeper, sweepBatchConfigSource);
        sweeperService.sweepPreviouslyConservativeNowThoroughTable(
                TABLE_1.getQualifiedName(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        assertThat(kvs.get(TABLE_1, readMap)).isEmpty();
    }

    @Test
    public void previouslyConservativeThrowsIfTableIsStillConservativelySwept() {
        createTable(TABLE_1, SweepStrategy.CONSERVATIVE);

        SweeperService sweeperService = new SweeperServiceImpl(specificTableSweeper, sweepBatchConfigSource);
        assertThatThrownBy(() -> sweeperService.sweepPreviouslyConservativeNowThoroughTable(
                        TABLE_1.getQualifiedName(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("it is not safe to run this type of sweep on conservatively swept tables");
    }

    @Test
    public void previouslyConservativeRespectsSkipPeriodWhenErasingSentinels() {
        skipCellVersion.setPeriod(4);
        createTable(TABLE_1, SweepStrategy.THOROUGH);

        Map<Cell, byte[]> sentinelWrites = IntStream.range(0, 100)
                .mapToObj(PtBytes::toBytes)
                .map(bytes -> Cell.create(bytes, bytes))
                .collect(Collectors.toMap(cell -> cell, _ignore -> PtBytes.EMPTY_BYTE_ARRAY));
        kvs.put(TABLE_1, sentinelWrites, Value.INVALID_VALUE_TIMESTAMP);
        Map<Cell, Long> readMap = KeyedStream.stream(sentinelWrites)
                .map(_ignore -> Long.MAX_VALUE)
                .collectToMap();
        assertThat(kvs.get(TABLE_1, readMap)).hasSize(100);

        SweeperService sweeperService = new SweeperServiceImpl(specificTableSweeper, sweepBatchConfigSource);
        sweeperService.sweepPreviouslyConservativeNowThoroughTable(
                TABLE_1.getQualifiedName(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        // Impl specific, documenting here -- exact last row will be swept twice, so the sentinel will be erased on
        // second pass-through; this is fine, not worth the risk of modifying behaviour
        assertThat(kvs.get(TABLE_1, readMap)).hasSize(24);
    }

    @Test
    public void previouslyConservativeRespectsSkipPeriodWhenSweepingNormally() {
        skipCellVersion.setPeriod(9);
        createTable(TABLE_1, SweepStrategy.THOROUGH);

        Map<Cell, byte[]> writes = KeyedStream.of(IntStream.range(0, 1000).boxed())
                .mapKeys(PtBytes::toBytes)
                .mapKeys(bytes -> Cell.create(bytes, bytes))
                .map(count -> count < 500 ? PtBytes.toBytes(count) : PtBytes.EMPTY_BYTE_ARRAY)
                .collectToMap();
        kvs.put(TABLE_1, writes, 100L);
        txService.putUnlessExists(100L, 101L);
        kvs.put(TABLE_1, writes, 103L);
        txService.putUnlessExists(103L, 104L);

        Map<Cell, Long> readMap =
                KeyedStream.stream(writes).map(_ignore -> 102L).collectToMap();
        assertThat(kvs.get(TABLE_1, readMap)).hasSize(1000);

        SweeperService sweeperService = new SweeperServiceImpl(specificTableSweeper, sweepBatchConfigSource);
        sweeperService.sweepPreviouslyConservativeNowThoroughTable(
                TABLE_1.getQualifiedName(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        // deletes all but a ninth of the entries at the lower timestamp
        assertThat(kvs.get(TABLE_1, readMap)).hasSize(110);
        Map<Cell, Long> readsAtMaxTs =
                KeyedStream.stream(readMap).map(_ignore -> Long.MAX_VALUE).collectToMap();
        // none of the cells at lower timestamp became visible
        assertThat(kvs.get(TABLE_1, readsAtMaxTs).values().stream()
                        .map(Value::getTimestamp)
                        .filter(timestamp -> timestamp == 100L)
                        .collect(Collectors.toList()))
                .hasSize(0);
        // 500 cannot be completely removed because they are not deletes, 111 of the remaining 500 have either of the
        // two versions skipped from sweep, which results in a version here (also allow off-by-one discrepancy as
        // mentioned in the test above)
        assertThat(kvs.get(TABLE_1, readsAtMaxTs)).hasSize(500 + 111);
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

    private static final class DeterministicTrueSupplier implements BooleanSupplier {
        private int truePeriod = 1;
        private long count = 0;

        private DeterministicTrueSupplier() {}

        public void setPeriod(int period) {
            truePeriod = period;
        }

        @Override
        public boolean getAsBoolean() {
            ++count;
            return count % truePeriod == 0;
        }
    }
}
