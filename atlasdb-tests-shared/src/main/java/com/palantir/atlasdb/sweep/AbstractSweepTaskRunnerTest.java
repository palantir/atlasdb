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
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.KvsManager;
import com.palantir.atlasdb.keyvalue.impl.TransactionManagerManager;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.util.Pair;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public abstract class AbstractSweepTaskRunnerTest extends AbstractSweepTest {
    protected static final int DEFAULT_BATCH_SIZE = 1000;

    protected SweepTaskRunner sweepRunner;
    protected LongSupplier tsSupplier;
    protected final AtomicLong sweepTimestamp = new AtomicLong();

    public AbstractSweepTaskRunnerTest(KvsManager kvsManager, TransactionManagerManager tmManager) {
        super(kvsManager, tmManager);
    }

    @Before
    @Override
    public void setup() {
        super.setup();
        tsSupplier = sweepTimestamp::get;

        CellsSweeper cellsSweeper = new CellsSweeper(txManager, kvs, ImmutableList.of());
        sweepRunner = new SweepTaskRunner(kvs, tsSupplier, tsSupplier, txService, cellsSweeper);
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
                PtBytes.EMPTY_BYTE_ARRAY,
                SweepTaskRunner.RunType.FULL);
        assertThat(results).isEqualTo(SweepResults.createEmptySweepResult(Optional.empty()));
        assertThat(getAllTsFromDefaultColumn("foo")).isEqualTo(ImmutableSet.of(50L, 75L, 100L, 125L, 150L));
    }

    @Test(timeout = 50000)
    public void testSweepBatchesDownToDeleteBatchSize() {
        CellsSweeper cellsSweeper = Mockito.mock(CellsSweeper.class);
        SweepTaskRunner spiedSweepRunner = new SweepTaskRunner(kvs, tsSupplier, tsSupplier, txService, cellsSweeper);

        putTwoValuesInEachCell(SMALL_LIST_OF_CELLS);

        int deleteBatchSize = 1;
        Pair<List<List<Cell>>, SweepResults> sweptCellsAndSweepResults =
                runSweep(cellsSweeper, spiedSweepRunner, 8, 8, deleteBatchSize);
        List<List<Cell>> sweptCells = sweptCellsAndSweepResults.getLhSide();
        assertThat(sweptCells).allMatch(list -> list.size() <= 2 * deleteBatchSize);
        assertThat(Iterables.concat(sweptCells)).containsExactlyElementsOf(SMALL_LIST_OF_CELLS);
    }

    @Test(timeout = 50000)
    public void testSweepBatchesUpToDeleteBatchSize() {
        CellsSweeper cellsSweeper = Mockito.mock(CellsSweeper.class);
        SweepTaskRunner spiedSweepRunner = new SweepTaskRunner(kvs, tsSupplier, tsSupplier, txService, cellsSweeper);

        putTwoValuesInEachCell(SMALL_LIST_OF_CELLS);

        Pair<List<List<Cell>>, SweepResults> sweptCellsAndSweepResults =
                runSweep(cellsSweeper, spiedSweepRunner, 8, 1, 4);
        List<List<Cell>> sweptCells = sweptCellsAndSweepResults.getLhSide();

        assertThat(sweptCells).hasSize(1);
        assertThat(sweptCells.get(0)).isEqualTo(SMALL_LIST_OF_CELLS);
    }

    @Test(timeout = 50000)
    public void testSweepBatches() {
        CellsSweeper cellsSweeper = Mockito.mock(CellsSweeper.class);
        SweepTaskRunner spiedSweepRunner = new SweepTaskRunner(kvs, tsSupplier, tsSupplier, txService, cellsSweeper);

        putTwoValuesInEachCell(BIG_LIST_OF_CELLS);

        int deleteBatchSize = 2;
        Pair<List<List<Cell>>, SweepResults> sweptCellsAndSweepResults =
                runSweep(cellsSweeper, spiedSweepRunner, 1000, 1, deleteBatchSize);
        List<List<Cell>> sweptCells = sweptCellsAndSweepResults.getLhSide();
        SweepResults sweepResults = sweptCellsAndSweepResults.getRhSide();
        assertThat(Iterables.concat(sweptCells)).containsExactlyElementsOf(BIG_LIST_OF_CELLS);
        for (List<Cell> sweptBatch : sweptCells.subList(0, sweptCells.size() - 1)) {
            // We requested deleteBatchSize = 2, so we expect between 2 and 4 timestamps deleted at a time.
            // We also expect a single timestamp to be swept per each cell.
            assertThat(sweptBatch.size()).isBetween(deleteBatchSize, 2 * deleteBatchSize);
        }
        // The last batch can be smaller than deleteBatchSize
        assertThat(sweptCells.get(sweptCells.size() - 1)).hasSizeLessThanOrEqualTo(2 * deleteBatchSize);

        assertThat(sweepResults.getCellTsPairsExamined())
                .describedAs("Expected Ts Pairs Examined should add up to entire table (2 values in each cell)")
                .isEqualTo(2 * BIG_LIST_OF_CELLS.size());
    }

    @Test(timeout = 50000)
    public void testSweepBatchesInDifferentRows() {
        CellsSweeper cellsSweeper = Mockito.mock(CellsSweeper.class);
        SweepTaskRunner spiedSweepRunner = new SweepTaskRunner(kvs, tsSupplier, tsSupplier, txService, cellsSweeper);

        putTwoValuesInEachCell(BIG_LIST_OF_CELLS_IN_DIFFERENT_ROWS);

        int deleteBatchSize = 2;
        Pair<List<List<Cell>>, SweepResults> sweptCellsAndSweepResults =
                runSweep(cellsSweeper, spiedSweepRunner, 10, 1, deleteBatchSize);
        List<List<Cell>> sweptCells = sweptCellsAndSweepResults.getLhSide();
        SweepResults sweepResults = sweptCellsAndSweepResults.getRhSide();
        assertThat(Iterables.concat(sweptCells)).containsExactlyElementsOf(BIG_LIST_OF_CELLS_IN_DIFFERENT_ROWS);
        for (List<Cell> sweptBatch : sweptCells.subList(0, sweptCells.size() - 1)) {
            // We requested deleteBatchSize = 2, so we expect between 2 and 4 timestamps deleted at a time.
            // We also expect a single timestamp to be swept per each cell.
            assertThat(sweptBatch.size()).isBetween(deleteBatchSize, 2 * deleteBatchSize);
        }
        // The last batch can be smaller than deleteBatchSize
        assertThat(sweptCells.get(sweptCells.size() - 1)).hasSizeLessThanOrEqualTo(2 * deleteBatchSize);

        assertThat(sweepResults.getCellTsPairsExamined())
                .describedAs("Expected Ts Pairs Examined should add up to entire table (2 values in each cell)")
                .isEqualTo(2 * BIG_LIST_OF_CELLS_IN_DIFFERENT_ROWS.size());
    }

    @Test(timeout = 50000)
    public void testSweepUncommittedConservative() {
        createTable(SweepStrategy.CONSERVATIVE);
        putIntoDefaultColumn("foo", "bar", 50);
        putUncommitted("foo", "baz", 100);
        SweepResults results = completeSweep(175).get();
        assertThat(results.getStaleValuesDeleted()).isEqualTo(1);
        assertThat(results.getCellTsPairsExamined()).isGreaterThanOrEqualTo(2);
        assertThat(getFromDefaultColumn("foo", 750)).isEqualTo("bar");
        assertThat(getAllTsFromDefaultColumn("foo")).isEqualTo(ImmutableSet.of(50L));
    }

    @Test(timeout = 50000)
    public void testSweepManyValuesThorough() {
        createTable(SweepStrategy.THOROUGH);
        putIntoDefaultColumn("foo", "bar", 50);
        putUncommitted("foo", "bad", 75);
        putIntoDefaultColumn("foo", "baz", 100);
        putIntoDefaultColumn("foo", "buzz", 125);
        putUncommitted("foo", "foo", 150);
        SweepResults results = completeSweep(175).get();
        assertThat(results.getStaleValuesDeleted()).isEqualTo(4);
        assertThat(results.getCellTsPairsExamined()).isGreaterThanOrEqualTo(5);
        assertThat(getFromDefaultColumn("foo", 200)).isEqualTo("buzz");
        assertThat(getFromDefaultColumn("foo", 124)).isNull();
        assertThat(getAllTsFromDefaultColumn("foo")).isEqualTo(ImmutableSet.of(125L));
    }

    @Test(timeout = 50000)
    public void testSweepManyValuesIncludingUncommittedConservative() {
        createTable(SweepStrategy.CONSERVATIVE);
        putIntoDefaultColumn("foo", "bar", 50);
        putUncommitted("foo", "bad", 75);
        putIntoDefaultColumn("foo", "baz", 100);
        putIntoDefaultColumn("foo", "buzz", 125);
        putUncommitted("foo", "foo", 150);
        SweepResults results = completeSweep(175).get();
        assertThat(results.getStaleValuesDeleted()).isEqualTo(4);
        assertThat(results.getCellTsPairsExamined()).isGreaterThanOrEqualTo(5);
        assertThat(getFromDefaultColumn("foo", 200)).isEqualTo("buzz");
        assertThat(getFromDefaultColumn("foo", 124)).isEmpty();
        assertThat(getAllTsFromDefaultColumn("foo")).isEqualTo(ImmutableSet.of(-1L, 125L));
    }

    @Test(timeout = 50000)
    public void testSweepUncommittedThorough() {
        createTable(SweepStrategy.THOROUGH);
        putIntoDefaultColumn("foo", "bar", 50);
        putUncommitted("foo", "baz", 100);
        SweepResults results = completeSweep(175).get();
        assertThat(results.getStaleValuesDeleted()).isEqualTo(1);
        assertThat(results.getCellTsPairsExamined()).isGreaterThanOrEqualTo(2);
        assertThat(getFromDefaultColumn("foo", 750)).isEqualTo("bar");
        assertThat(getAllTsFromDefaultColumn("foo")).isEqualTo(ImmutableSet.of(50L));
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
                nextStartRow,
                SweepTaskRunner.RunType.FULL);
        assertThat(results).isEqualTo(SweepResults.createEmptySweepResult(Optional.empty()));
    }

    @Test(timeout = 50000)
    public void testSweepManyLatestDeletedThoroughIncludingUncommitted1() {
        createTable(SweepStrategy.THOROUGH);
        putIntoDefaultColumn("foo", "bar", 50);
        putUncommitted("foo", "bad", 75);
        putIntoDefaultColumn("foo", "baz", 100);
        putIntoDefaultColumn("foo", "", 125);
        putUncommitted("foo", "foo", 150);
        putIntoDefaultColumn("zzz", "bar", 51);

        SweepResults results = completeSweep(175).get();
        assertThat(results.getStaleValuesDeleted()).isEqualTo(4);
        assertThat(results.getCellTsPairsExamined()).isGreaterThanOrEqualTo(6);
        // this check is a nuance of SweepTaskRunner: the value at timestamp 125 is actually eligible for deletion,
        // but we don't delete it on the first pass due to the later uncommitted value. below we sweep again and make
        // sure it's deleted
        assertThat(getFromDefaultColumn("foo", 200)).isEmpty();
        assertThat(getAllTsFromDefaultColumn("foo")).isEqualTo(ImmutableSet.of(125L));

        results = completeSweep(175).get();
        assertThat(results.getStaleValuesDeleted()).isEqualTo(1);
        assertThat(results.getCellTsPairsExamined()).isGreaterThanOrEqualTo(2);
        assertThat(getFromDefaultColumn("foo", 200)).isNull();
        assertThat(getAllTsFromDefaultColumn("foo")).isEmpty();
    }

    @Test(timeout = 50000)
    public void testSweepManyLatestDeletedThoroughIncludingUncommitted2() {
        createTable(SweepStrategy.THOROUGH);
        putIntoDefaultColumn("foo", "bar", 50);
        putUncommitted("foo", "bad", 75);
        putIntoDefaultColumn("foo", "baz", 100);
        putIntoDefaultColumn("foo", "foo", 125);
        putUncommitted("foo", "", 150);
        SweepResults results = completeSweep(175).get();
        assertThat(results.getStaleValuesDeleted()).isEqualTo(4);
        assertThat(results.getCellTsPairsExamined()).isGreaterThanOrEqualTo(5);
        assertThat(getFromDefaultColumn("foo", 200)).isEqualTo("foo");
        assertThat(getAllTsFromDefaultColumn("foo")).isEqualTo(ImmutableSet.of(125L));
    }

    @Test
    public void testSweepHighlyVersionedCell() {
        createTable(TableMetadataPersistence.SweepStrategy.CONSERVATIVE);

        Multimap<Cell, Value> commits = HashMultimap.create();
        for (int i = 1; i <= 1_000; i++) {
            putUncommitted("row", RandomStringUtils.random(10), i);
            Cell tsCell =
                    Cell.create(TransactionConstants.getValueForTimestamp(i), TransactionConstants.COMMIT_TS_COLUMN);
            commits.put(tsCell, Value.create(TransactionConstants.getValueForTimestamp(i), 0));
        }
        kvs.putWithTimestamps(TransactionConstants.TRANSACTION_TABLE, commits);

        Optional<SweepResults> results = completeSweep(TABLE_NAME, 100_000, 1);
        assertThat(results.get().getStaleValuesDeleted()).isEqualTo(1_000 - 1);
    }

    @Test
    public void shouldReturnValuesForMultipleColumnsWhenSweeping() {
        createTable(TableMetadataPersistence.SweepStrategy.CONSERVATIVE);

        for (int ts = 10; ts <= 150; ts += 10) {
            put("row", "col1", "value", ts);
            put("row", "col2", "value", ts + 5);
        }

        SweepResults results = completeSweep(350).get();
        assertThat(results.getStaleValuesDeleted()).isEqualTo(28);
    }

    @SuppressWarnings("unchecked")
    private Pair<List<List<Cell>>, SweepResults> runSweep(
            CellsSweeper cellsSweeper,
            SweepTaskRunner spiedSweepRunner,
            int maxCellTsPairsToExamine,
            int candidateBatchSize,
            int deleteBatchSize) {
        sweepTimestamp.set(Long.MAX_VALUE);
        List<List<Cell>> sweptCells = new ArrayList<>();

        doAnswer(invocationOnMock -> {
                    Object[] arguments = invocationOnMock.getArguments();
                    Collection<Cell> sentinelsToAdd = (Collection<Cell>) arguments[2];
                    sweptCells.add(new ArrayList<>(sentinelsToAdd));
                    return null;
                })
                .when(cellsSweeper)
                .sweepCells(eq(TABLE_NAME), any(), any());

        SweepResults sweepResults = spiedSweepRunner.run(
                TABLE_NAME,
                ImmutableSweepBatchConfig.builder()
                        .maxCellTsPairsToExamine(maxCellTsPairsToExamine)
                        .candidateBatchSize(candidateBatchSize)
                        .deleteBatchSize(deleteBatchSize)
                        .build(),
                PtBytes.EMPTY_BYTE_ARRAY,
                SweepTaskRunner.RunType.FULL);

        return new Pair<>(sweptCells, sweepResults);
    }

    @Override
    protected Optional<SweepResults> completeSweep(long ts) {
        return completeSweep(TABLE_NAME, ts);
    }

    @Override
    protected Optional<SweepResults> completeSweep(TableReference tableReference, long ts) {
        return completeSweep(tableReference, ts, DEFAULT_BATCH_SIZE);
    }

    protected Optional<SweepResults> completeSweep(TableReference tableReference, long ts, int batchSize) {
        sweepTimestamp.set(ts);
        byte[] startRow = PtBytes.EMPTY_BYTE_ARRAY;
        long totalStaleValuesDeleted = 0;
        long totalCellsExamined = 0;
        for (int run = 0; run < 100; ++run) {
            SweepResults results = sweepRunner.run(
                    tableReference,
                    ImmutableSweepBatchConfig.builder()
                            .deleteBatchSize(batchSize)
                            .candidateBatchSize(batchSize)
                            .maxCellTsPairsToExamine(batchSize)
                            .build(),
                    startRow,
                    SweepTaskRunner.RunType.FULL);
            assertThat(results.getMinSweptTimestamp()).isEqualTo(ts);
            assertThat(results.getPreviousStartRow().orElse(null)).isEqualTo(startRow);
            totalStaleValuesDeleted += results.getStaleValuesDeleted();
            totalCellsExamined += results.getCellTsPairsExamined();
            if (!results.getNextStartRow().isPresent()) {
                return Optional.of(SweepResults.builder()
                        .staleValuesDeleted(totalStaleValuesDeleted)
                        .cellTsPairsExamined(totalCellsExamined)
                        .timeInMillis(1)
                        .timeSweepStarted(1)
                        .minSweptTimestamp(ts)
                        .build());
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
                PtBytes.EMPTY_BYTE_ARRAY,
                SweepTaskRunner.RunType.FULL);
        assertThat(results.getNextStartRow()).isPresent();
        return results;
    }
}
