/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ImmutableSweepResults;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.SweepStatsKeyValueService;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManagers;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.transaction.service.TransactionServices;
import com.palantir.timestamp.InMemoryTimestampService;
import com.palantir.timestamp.TimestampService;

public abstract class AbstractSweepTaskRunnerTest {
    private static final String FULL_TABLE_NAME = "test_table.xyz_atlasdb_sweeper_test";
    protected static final TableReference TABLE_NAME = TableReference.createFromFullyQualifiedName(FULL_TABLE_NAME);
    private static final String COL = "c";
    protected static final int DEFAULT_BATCH_SIZE = 1000;

    private static final List<Cell> SMALL_LIST_OF_CELLS = Lists.newArrayList();
    private static final List<Cell> BIG_LIST_OF_CELLS = Lists.newArrayList();

    static {
        for (int i = 0; i < 10; i++) {
            String colName = String.format("c%d", i);
            BIG_LIST_OF_CELLS.add(
                    Cell.create("row".getBytes(StandardCharsets.UTF_8), colName.getBytes(StandardCharsets.UTF_8)));
        }
        SMALL_LIST_OF_CELLS.addAll(BIG_LIST_OF_CELLS.subList(0, 4));
    }

    protected KeyValueService kvs;
    protected final AtomicLong sweepTimestamp = new AtomicLong();
    protected SweepTaskRunner sweepRunner;
    protected LockAwareTransactionManager txManager;
    protected TransactionService txService;
    protected PersistentLockManager persistentLockManager;
    protected SweepStrategyManager ssm;
    protected LongSupplier tsSupplier;

    @Before
    public void setup() {
        TimestampService tsService = new InMemoryTimestampService();
        kvs = SweepStatsKeyValueService.create(getKeyValueService(), tsService);
        ssm = SweepStrategyManagers.createDefault(kvs);
        txService = TransactionServices.createTransactionService(kvs);
        txManager = SweepTestUtils.setupTxManager(kvs, tsService, ssm, txService);
        tsSupplier = sweepTimestamp::get;
        persistentLockManager = new PersistentLockManager(
                SweepTestUtils.getPersistentLockService(kvs),
                AtlasDbConstants.DEFAULT_SWEEP_PERSISTENT_LOCK_WAIT_MILLIS);
        CellsSweeper cellsSweeper = new CellsSweeper(txManager, kvs, persistentLockManager, ImmutableList.of());
        sweepRunner = new SweepTaskRunner(kvs, tsSupplier, tsSupplier, txService, ssm, cellsSweeper);
    }

    @After
    public void close() {
        kvs.close();
    }

    /**
     * Called once before each test.
     *
     * @return the KVS used for testing
     */
    protected abstract KeyValueService getKeyValueService();

    @Test(timeout = 50000)
    public void testSweepOneConservative() {
        createTable(SweepStrategy.CONSERVATIVE);
        putIntoDefaultColumn("foo", "bar", 50);
        putIntoDefaultColumn("foo", "baz", 100);
        SweepResults results = completeSweep(175);
        assertEquals(1, results.getStaleValuesDeleted());
        assertThat(results.getCellTsPairsExamined()).isGreaterThanOrEqualTo(2);
        assertEquals("baz", get("foo", 150));
        assertEquals("", get("foo", 80));
        assertEquals(ImmutableSet.of(-1L, 100L), getAllTs("foo"));
    }

    @Test(timeout = 50000)
    public void testDontSweepLatestConservative() {
        createTable(SweepStrategy.CONSERVATIVE);
        putIntoDefaultColumn("foo", "bar", 50);
        SweepResults results = completeSweep(75);
        assertEquals(0, results.getStaleValuesDeleted());
        assertThat(results.getCellTsPairsExamined()).isGreaterThanOrEqualTo(1);
        assertEquals("bar", get("foo", 150));
        assertEquals(ImmutableSet.of(50L), getAllTs("foo"));
    }

    @Test(timeout = 50000)
    public void testSweepUncommittedConservative() {
        createTable(SweepStrategy.CONSERVATIVE);
        putIntoDefaultColumn("foo", "bar", 50);
        putUncommitted("foo", "baz", 100);
        SweepResults results = completeSweep(175);
        assertEquals(1, results.getStaleValuesDeleted());
        assertThat(results.getCellTsPairsExamined()).isGreaterThanOrEqualTo(2);
        assertEquals("bar", get("foo", 750));
        assertEquals(ImmutableSet.of(50L), getAllTs("foo"));
    }

    @Test(timeout = 50000)
    public void testSweepManyValuesConservative() {
        createTable(SweepStrategy.CONSERVATIVE);
        putIntoDefaultColumn("foo", "bar", 50);
        putUncommitted("foo", "bad", 75);
        putIntoDefaultColumn("foo", "baz", 100);
        putIntoDefaultColumn("foo", "buzz", 125);
        putUncommitted("foo", "foo", 150);
        SweepResults results = completeSweep(175);
        assertEquals(4, results.getStaleValuesDeleted());
        assertThat(results.getCellTsPairsExamined()).isGreaterThanOrEqualTo(5);
        assertEquals("buzz", get("foo", 200));
        assertEquals("", get("foo", 124));
        assertEquals(ImmutableSet.of(-1L, 125L), getAllTs("foo"));
    }

    @Test(timeout = 50000)
    public void testSweepManyRowsConservative() {
        testSweepManyRows(SweepStrategy.CONSERVATIVE);
    }

    @Test(timeout = 50000)
    public void testDontSweepFutureConservative() {
        createTable(SweepStrategy.CONSERVATIVE);
        putIntoDefaultColumn("foo", "bar", 50);
        putUncommitted("foo", "bad", 75);
        putIntoDefaultColumn("foo", "baz", 100);
        putIntoDefaultColumn("foo", "buzz", 125);
        putUncommitted("foo", "foo", 150);
        SweepResults results = completeSweep(110);
        assertEquals(2, results.getStaleValuesDeleted());
        // Future timestamps don't count towards the examined count
        assertThat(results.getCellTsPairsExamined()).isGreaterThanOrEqualTo(3);
        assertEquals(ImmutableSet.of(-1L, 100L, 125L, 150L), getAllTs("foo"));
    }

    @Test(timeout = 50000)
    public void testSweepOneThorough() {
        createTable(SweepStrategy.THOROUGH);
        putIntoDefaultColumn("foo", "bar", 50);
        putIntoDefaultColumn("foo", "baz", 100);
        SweepResults results = completeSweep(175);
        assertEquals(1, results.getStaleValuesDeleted());
        assertThat(results.getCellTsPairsExamined()).isGreaterThanOrEqualTo(2);
        assertEquals("baz", get("foo", 150));
        assertNull(get("foo", 80));
        assertEquals(ImmutableSet.of(100L), getAllTs("foo"));
    }

    @Test(timeout = 50000)
    public void testDontSweepLatestThorough() {
        createTable(SweepStrategy.THOROUGH);
        putIntoDefaultColumn("foo", "bar", 50);
        SweepResults results = completeSweep(75);
        assertEquals(0, results.getStaleValuesDeleted());
        assertThat(results.getCellTsPairsExamined()).isGreaterThanOrEqualTo(1);
        assertEquals("bar", get("foo", 150));
        assertEquals(ImmutableSet.of(50L), getAllTs("foo"));
    }

    @Test(timeout = 50000)
    public void testSweepLatestDeletedMultiRowThorough() {
        createTable(SweepStrategy.THOROUGH);
        putIntoDefaultColumn("foo", "", 50);
        put("foo-2", "other", "womp", 60);
        SweepResults results = completeSweep(75);
        assertEquals(1, results.getStaleValuesDeleted());
        assertThat(results.getCellTsPairsExamined()).isGreaterThanOrEqualTo(1);

        assertNull(get("foo", 150));
        assertEquals(ImmutableSet.of(), getAllTs("foo"));
    }

    @Test(timeout = 50000)
    public void testSweepLatestDeletedMultiColThorough() {
        createTable(SweepStrategy.THOROUGH);
        put("foo", "The Library", "Donna Noble has been saved", 40);
        putIntoDefaultColumn("foo", "", 50);
        SweepResults results = completeSweep(75);
        assertEquals(1, results.getStaleValuesDeleted());
        assertThat(results.getCellTsPairsExamined()).isGreaterThanOrEqualTo(1);

        // TODO these look wrong but are actually OK - they check only for COL
        assertNull(get("foo", 150));
        assertEquals(ImmutableSet.of(), getAllTs("foo"));
    }

    @Test(timeout = 50000)
    public void testSweepLatestDeletedMultiValConservative() {
        createTable(SweepStrategy.CONSERVATIVE);
        putIntoDefaultColumn("foo", "this citizen will be erased", 40);
        putIntoDefaultColumn("foo", "", 50);
        SweepResults results = completeSweep(75);
        assertEquals(1, results.getStaleValuesDeleted());
        assertThat(results.getCellTsPairsExamined()).isGreaterThanOrEqualTo(1);
        assertNull(get("foo", 150));
        assertEquals(ImmutableSet.of(), getAllTs("foo"));
    }

    @Test(timeout = 50000)
    public void testSweepLatestNotDeletedMultiValThorough() {
        createTable(SweepStrategy.THOROUGH);
        putIntoDefaultColumn("foo", "this citizen will be erased", 40);
        putIntoDefaultColumn("foo", "this is the modern world", 50);
        SweepResults results = completeSweep(75);
        assertEquals(1, results.getStaleValuesDeleted());
        assertThat(results.getCellTsPairsExamined()).isGreaterThanOrEqualTo(1);
//        assertNull(get("foo", 150));
//        assertEquals(ImmutableSet.of(), getAllTs("foo"));
    }

    @Test(timeout = 50000)
    public void testSweepLatestDeletedMultiValThorough() {
        createTable(SweepStrategy.THOROUGH);
        putIntoDefaultColumn("foo", "this citizen will be erased", 40);
        putIntoDefaultColumn("foo", "", 50);
        SweepResults results = completeSweep(75);
        assertEquals(1, results.getStaleValuesDeleted());
        assertThat(results.getCellTsPairsExamined()).isGreaterThanOrEqualTo(1);
        assertNull(get("foo", 150));
        assertEquals(ImmutableSet.of(), getAllTs("foo"));
    }

    @Test(timeout = 50000)
    public void testSweepLatestDeletedThorough() {
        createTable(SweepStrategy.THOROUGH);
        putIntoDefaultColumn("foo", "", 50);
        SweepResults results = completeSweep(75);
        assertEquals(1, results.getStaleValuesDeleted());
        assertThat(results.getCellTsPairsExamined()).isGreaterThanOrEqualTo(1);
        assertNull(get("foo", 150));
        assertEquals(ImmutableSet.of(), getAllTs("foo"));
    }

    @Test(timeout = 50000)
    public void testSweepUncommittedThorough() {
        createTable(SweepStrategy.THOROUGH);
        putIntoDefaultColumn("foo", "bar", 50);
        putUncommitted("foo", "baz", 100);
        SweepResults results = completeSweep(175);
        assertEquals(1, results.getStaleValuesDeleted());
        assertThat(results.getCellTsPairsExamined()).isGreaterThanOrEqualTo(2);
        assertEquals("bar", get("foo", 750));
        assertEquals(ImmutableSet.of(50L), getAllTs("foo"));
    }

    @Test(timeout = 50000)
    public void testSweepManyValuesThorough() {
        createTable(SweepStrategy.THOROUGH);
        putIntoDefaultColumn("foo", "bar", 50);
        putUncommitted("foo", "bad", 75);
        putIntoDefaultColumn("foo", "baz", 100);
        putIntoDefaultColumn("foo", "buzz", 125);
        putUncommitted("foo", "foo", 150);
        SweepResults results = completeSweep(175);
        assertEquals(4, results.getStaleValuesDeleted());
        assertThat(results.getCellTsPairsExamined()).isGreaterThanOrEqualTo(5);
        assertEquals("buzz", get("foo", 200));
        assertNull(get("foo", 124));
        assertEquals(ImmutableSet.of(125L), getAllTs("foo"));
    }

    @Test(timeout = 50000)
    public void testSweepManyRowsThorough() {
        testSweepManyRows(SweepStrategy.THOROUGH);
    }

    @Test(timeout = 50000)
    public void testSweepManyLatestDeletedThorough1() {
        createTable(SweepStrategy.THOROUGH);
        putIntoDefaultColumn("foo", "bar", 50);
        putUncommitted("foo", "bad", 75);
        putIntoDefaultColumn("foo", "baz", 100);
        putIntoDefaultColumn("foo", "", 125);
        putUncommitted("foo", "foo", 150);
        putIntoDefaultColumn("zzz", "bar", 51);

        SweepResults results = completeSweep(175);
        assertEquals(4, results.getStaleValuesDeleted());
        assertThat(results.getCellTsPairsExamined()).isGreaterThanOrEqualTo(6);
        assertEquals("", get("foo", 200));
        assertEquals(ImmutableSet.of(125L), getAllTs("foo"));

        results = completeSweep(175);
        assertEquals(1, results.getStaleValuesDeleted());
        assertThat(results.getCellTsPairsExamined()).isGreaterThanOrEqualTo(2);
        assertNull(get("foo", 200));
        assertEquals(ImmutableSet.of(), getAllTs("foo"));
    }

    @Test(timeout = 50000)
    public void testSweepManyLatestDeletedThorough2() {
        createTable(SweepStrategy.THOROUGH);
        putIntoDefaultColumn("foo", "bar", 50);
        putUncommitted("foo", "bad", 75);
        putIntoDefaultColumn("foo", "baz", 100);
        putIntoDefaultColumn("foo", "foo", 125);
        putUncommitted("foo", "", 150);
        SweepResults results = completeSweep(175);
        assertEquals(4, results.getStaleValuesDeleted());
        assertThat(results.getCellTsPairsExamined()).isGreaterThanOrEqualTo(5);
        assertEquals("foo", get("foo", 200));
        assertEquals(ImmutableSet.of(125L), getAllTs("foo"));
    }

    @Test(timeout = 50000)
    public void testDontSweepFutureThorough() {
        createTable(SweepStrategy.THOROUGH);
        putIntoDefaultColumn("foo", "bar", 50);
        putUncommitted("foo", "bad", 75);
        putIntoDefaultColumn("foo", "baz", 100);
        putIntoDefaultColumn("foo", "buzz", 125);
        putUncommitted("foo", "foo", 150);
        SweepResults results = completeSweep(110);
        assertEquals(2, results.getStaleValuesDeleted());
        // Future timestamps don't count towards the examined count
        assertThat(results.getCellTsPairsExamined()).isGreaterThanOrEqualTo(3);
        assertEquals(ImmutableSet.of(100L, 125L, 150L), getAllTs("foo"));
    }

    @Test(timeout = 50000)
    public void testSweepStrategyNothing() {
        createTable(SweepStrategy.NOTHING);
        putIntoDefaultColumn("foo", "bar", 50);
        putUncommitted("foo", "bad", 75);
        putIntoDefaultColumn("foo", "baz", 100);
        putIntoDefaultColumn("foo", "buzz", 125);
        putUncommitted("foo", "foo", 150);
        SweepResults results = sweepRunner.run(
                TABLE_NAME,
                ImmutableSweepBatchConfig.builder()
                        .deleteBatchSize(DEFAULT_BATCH_SIZE)
                        .candidateBatchSize(DEFAULT_BATCH_SIZE)
                        .maxCellTsPairsToExamine(DEFAULT_BATCH_SIZE)
                        .build(),
                PtBytes.EMPTY_BYTE_ARRAY);
        assertEquals(SweepResults.createEmptySweepResult(), results);
        assertEquals(ImmutableSet.of(50L, 75L, 100L, 125L, 150L), getAllTs("foo"));
    }

    @Test(timeout = 50000)
    public void testSweeperFailsHalfwayThroughOnDeleteTable() {
        createTable(SweepStrategy.CONSERVATIVE);
        putIntoDefaultColumn("foo", "bar", 50);
        putIntoDefaultColumn("foo2", "bang", 75);
        putIntoDefaultColumn("foo3", "baz", 100);
        putIntoDefaultColumn("foo4", "buzz", 125);
        byte[] nextStartRow = partialSweep(150).getNextStartRow().get();

        kvs.dropTable(TABLE_NAME);

        SweepResults results = sweepRunner.run(
                TABLE_NAME,
                ImmutableSweepBatchConfig.builder()
                        .deleteBatchSize(DEFAULT_BATCH_SIZE)
                        .candidateBatchSize(DEFAULT_BATCH_SIZE)
                        .maxCellTsPairsToExamine(DEFAULT_BATCH_SIZE)
                        .build(),
                nextStartRow);
        assertEquals(SweepResults.createEmptySweepResult(), results);
    }

    @Test(timeout = 50000)
    public void testSweepingAlreadySweptTable() {
        createTable(SweepStrategy.CONSERVATIVE);
        putIntoDefaultColumn("row", "val", 10);
        putIntoDefaultColumn("row", "val", 20);

        SweepResults results = completeSweep(30);
        assertEquals(1, results.getStaleValuesDeleted());
        assertThat(results.getCellTsPairsExamined()).isGreaterThanOrEqualTo(2);

        results = completeSweep(40);
        assertEquals(0, results.getStaleValuesDeleted());
        assertThat(results.getCellTsPairsExamined()).isGreaterThanOrEqualTo(1);
    }

    @Test(timeout = 50000)
    public void testSweepOnMixedCaseTable() {
        TableReference mixedCaseTable = TableReference.create(Namespace.create("someNamespace"), "someTable");
        createTable(mixedCaseTable, SweepStrategy.CONSERVATIVE);
        put(mixedCaseTable, "row", "col", "val", 10);
        put(mixedCaseTable, "row", "col", "val", 20);

        SweepResults results = completeSweep(mixedCaseTable, 30);
        assertEquals(1, results.getStaleValuesDeleted());
        assertThat(results.getCellTsPairsExamined()).isGreaterThanOrEqualTo(2);
    }

    @Test(timeout = 50000)
    public void testSweepBatchesDownToDeleteBatchSize() {
        CellsSweeper cellsSweeper = Mockito.mock(CellsSweeper.class);
        SweepTaskRunner spiedSweepRunner =
                new SweepTaskRunner(kvs, tsSupplier, tsSupplier, txService, ssm, cellsSweeper);

        putTwoValuesInEachCell(SMALL_LIST_OF_CELLS);

        int deleteBatchSize = 1;
        List<List<Cell>> sweptCells = runSweep(cellsSweeper, spiedSweepRunner,
                8, 8, deleteBatchSize);
        assertThat(sweptCells).allMatch(list -> list.size() <= 2 * deleteBatchSize);
        assertThat(Iterables.concat(sweptCells)).containsExactlyElementsOf(SMALL_LIST_OF_CELLS);
    }

    @Test(timeout = 50000)
    public void testSweepBatchesUpToDeleteBatchSize() {
        CellsSweeper cellsSweeper = Mockito.mock(CellsSweeper.class);
        SweepTaskRunner spiedSweepRunner =
                new SweepTaskRunner(kvs, tsSupplier, tsSupplier, txService, ssm, cellsSweeper);

        putTwoValuesInEachCell(SMALL_LIST_OF_CELLS);

        List<List<Cell>> sweptCells = runSweep(cellsSweeper, spiedSweepRunner, 8, 1, 4);

        assertEquals(1, sweptCells.size());
        assertEquals(SMALL_LIST_OF_CELLS, sweptCells.get(0));
    }

    @Test(timeout = 50000)
    public void testSweepBatches() {
        CellsSweeper cellsSweeper = Mockito.mock(CellsSweeper.class);
        SweepTaskRunner spiedSweepRunner =
                new SweepTaskRunner(kvs, tsSupplier, tsSupplier, txService, ssm, cellsSweeper);

        putTwoValuesInEachCell(BIG_LIST_OF_CELLS);

        int deleteBatchSize = 2;
        List<List<Cell>> sweptCells = runSweep(cellsSweeper, spiedSweepRunner,
                1000, 1, deleteBatchSize);
        assertThat(Iterables.concat(sweptCells)).containsExactlyElementsOf(BIG_LIST_OF_CELLS);
        for (List<Cell> sweptBatch : sweptCells.subList(0, sweptCells.size() - 1)) {
            // We requested deleteBatchSize = 2, so we expect between 2 and 4 timestamps deleted at a time.
            // We also expect a single timestamp to be swept per each cell.
            assertThat(sweptBatch.size()).isBetween(deleteBatchSize, 2 * deleteBatchSize);
        }
        // The last batch can be smaller than deleteBatchSize
        assertThat(sweptCells.get(sweptCells.size() - 1).size()).isLessThanOrEqualTo(2 * deleteBatchSize);
    }

    private void putTwoValuesInEachCell(List<Cell> cells) {
        createTable(SweepStrategy.CONSERVATIVE);

        int ts = 1;
        for (Cell cell : cells) {
            put(cell, "val1", ts);
            put(cell, "val2", ts + 5);
            ts += 10;
        }

        sweepTimestamp.set(ts);
    }

    @SuppressWarnings("unchecked")
    private List<List<Cell>> runSweep(CellsSweeper cellsSweeper, SweepTaskRunner spiedSweepRunner,
            int maxCellTsPairsToExamine, int candidateBatchSize, int deleteBatchSize) {
        List<List<Cell>> sweptCells = Lists.newArrayList();

        doAnswer((invocationOnMock) -> {
            Object[] arguments = invocationOnMock.getArguments();
            Collection<Cell> sentinelsToAdd = (Collection<Cell>) arguments[2];
            sweptCells.add(new ArrayList(sentinelsToAdd));
            return null;
        }).when(cellsSweeper).sweepCells(eq(TABLE_NAME), any(), any());

        spiedSweepRunner.run(TABLE_NAME, ImmutableSweepBatchConfig.builder()
                .maxCellTsPairsToExamine(maxCellTsPairsToExamine)
                .candidateBatchSize(candidateBatchSize)
                .deleteBatchSize(deleteBatchSize)
                .build(), PtBytes.EMPTY_BYTE_ARRAY);

        return sweptCells;
    }

    private void testSweepManyRows(SweepStrategy strategy) {
        createTable(strategy);
        putIntoDefaultColumn("foo", "bar1", 5);
        putIntoDefaultColumn("foo", "bar2", 10);
        putIntoDefaultColumn("baz", "bar3", 15);
        putIntoDefaultColumn("baz", "bar4", 20);
        SweepResults results = completeSweep(175);
        assertEquals(2, results.getStaleValuesDeleted());
        assertThat(results.getCellTsPairsExamined()).isGreaterThanOrEqualTo(4);
    }

    protected SweepResults completeSweep(long ts) {
        return completeSweep(TABLE_NAME, ts);
    }

    protected SweepResults completeSweep(TableReference tableReference, long ts) {
        sweepTimestamp.set(ts);
        byte[] startRow = PtBytes.EMPTY_BYTE_ARRAY;
        long totalStaleValuesDeleted = 0;
        long totalCellsExamined = 0;
        for (int run = 0; run < 100; ++run) {
            SweepResults results = sweepRunner.run(
                    tableReference,
                    ImmutableSweepBatchConfig.builder()
                            .deleteBatchSize(DEFAULT_BATCH_SIZE)
                            .candidateBatchSize(DEFAULT_BATCH_SIZE)
                            .maxCellTsPairsToExamine(DEFAULT_BATCH_SIZE)
                            .build(),
                    startRow);
            assertEquals(ts, results.getSweptTimestamp());
            assertArrayEquals(startRow, results.getPreviousStartRow().orElse(null));
            totalStaleValuesDeleted += results.getStaleValuesDeleted();
            totalCellsExamined += results.getCellTsPairsExamined();
            if (!results.getNextStartRow().isPresent()) {
                return ImmutableSweepResults.builder()
                        .staleValuesDeleted(totalStaleValuesDeleted)
                        .cellTsPairsExamined(totalCellsExamined)
                        .sweptTimestamp(ts)
                        .build();
            }
            startRow = results.getNextStartRow().get();
        }
        fail("failed to completely sweep a table in 100 runs");
        return null; // should never be reached
    }

    private SweepResults partialSweep(long ts) {
        sweepTimestamp.set(ts);

        SweepResults results = sweepRunner.run(
                TABLE_NAME,
                ImmutableSweepBatchConfig.builder()
                        .deleteBatchSize(1)
                        .candidateBatchSize(1)
                        .maxCellTsPairsToExamine(1)
                        .build(),
                PtBytes.EMPTY_BYTE_ARRAY);
        assertTrue(results.getNextStartRow().isPresent());
        return results;
    }

    private String get(String row, long ts) {
        Cell cell = Cell.create(row.getBytes(StandardCharsets.UTF_8), COL.getBytes(StandardCharsets.UTF_8));
        Value val = kvs.get(TABLE_NAME, ImmutableMap.of(cell, ts)).get(cell);
        return val == null ? null : new String(val.getContents(), StandardCharsets.UTF_8);
    }

    private Set<Long> getAllTs(String row) {
        Cell cell = Cell.create(row.getBytes(StandardCharsets.UTF_8), COL.getBytes(StandardCharsets.UTF_8));
        return ImmutableSet.copyOf(kvs.getAllTimestamps(TABLE_NAME, ImmutableSet.of(cell), Long.MAX_VALUE).get(cell));
    }

    protected void putIntoDefaultColumn(final String row, final String val, final long ts) {
        put(row, COL, val, ts);
    }

    protected void put(final String row, final String column, final String val, final long ts) {
        put(TABLE_NAME, row, column, val, ts);
    }

    protected void put(final TableReference tableRef,
            final String row,
            final String column,
            final String val,
            final long ts) {
        Cell cell = Cell.create(row.getBytes(StandardCharsets.UTF_8), column.getBytes(StandardCharsets.UTF_8));
        put(tableRef, cell, val, ts);
    }

    protected void put(Cell cell, final String val, final long ts) {
        put(TABLE_NAME, cell, val, ts);
    }

    protected void put(final TableReference tableRef, Cell cell, final String val, final long ts) {
        kvs.put(tableRef, ImmutableMap.of(cell, val.getBytes(StandardCharsets.UTF_8)), ts);
        putTimestampIntoTransactionTable(ts);
    }

    private void putTimestampIntoTransactionTable(long ts) {
        txService.putUnlessExists(ts, ts);
    }

    private void putUncommitted(final String row, final String val, final long ts) {
        Cell cell = Cell.create(row.getBytes(StandardCharsets.UTF_8), COL.getBytes(StandardCharsets.UTF_8));
        kvs.put(TABLE_NAME, ImmutableMap.of(cell, val.getBytes(StandardCharsets.UTF_8)), ts);
    }

    protected void createTable(SweepStrategy sweepStrategy) {
        createTable(TABLE_NAME, sweepStrategy);
    }

    protected void createTable(TableReference tableReference, SweepStrategy sweepStrategy) {
        kvs.createTable(tableReference,
                new TableDefinition() {
                    {
                        rowName();
                        rowComponent("row", ValueType.BLOB);
                        columns();
                        column("col", COL, ValueType.BLOB);
                        conflictHandler(ConflictHandler.IGNORE_ALL);
                        sweepStrategy(sweepStrategy);
                    }
                }.toTableMetadata().persistToBytes()
        );
    }
}
