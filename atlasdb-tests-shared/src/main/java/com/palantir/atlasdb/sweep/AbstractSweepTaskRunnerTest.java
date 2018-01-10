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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ImmutableSweepResults;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy;

public abstract class AbstractSweepTaskRunnerTest extends AbstractSweepTest {
    protected static final int DEFAULT_BATCH_SIZE = 1000;

    protected SweepTaskRunner sweepRunner;
    protected LongSupplier tsSupplier;
    protected final AtomicLong sweepTimestamp = new AtomicLong();

    @Before
    public void setup() {
        super.setup();
        tsSupplier = sweepTimestamp::get;

        CellsSweeper cellsSweeper = new CellsSweeper(txManager, kvs, persistentLockManager, ImmutableList.of());
        sweepRunner = new SweepTaskRunner(kvs, tsSupplier, tsSupplier, txService, ssm, cellsSweeper);
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
        assertEquals(SweepResults.createEmptySweepResult(Optional.empty()), results);
        assertEquals(ImmutableSet.of(50L, 75L, 100L, 125L, 150L), getAllTsFromDefaultColumn("foo"));
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

    @Test(timeout = 50000)
    public void testSweepUncommittedConservative() {
        createTable(SweepStrategy.CONSERVATIVE);
        putIntoDefaultColumn("foo", "bar", 50);
        putUncommitted("foo", "baz", 100);
        SweepResults results = completeSweep(175).get();
        assertEquals(1, results.getStaleValuesDeleted());
        assertThat(results.getCellTsPairsExamined()).isGreaterThanOrEqualTo(2);
        assertEquals("bar", getFromDefaultColumn("foo", 750));
        assertEquals(ImmutableSet.of(50L), getAllTsFromDefaultColumn("foo"));
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
        assertEquals(4, results.getStaleValuesDeleted());
        assertThat(results.getCellTsPairsExamined()).isGreaterThanOrEqualTo(5);
        assertEquals("buzz", getFromDefaultColumn("foo", 200));
        assertNull(getFromDefaultColumn("foo", 124));
        assertEquals(ImmutableSet.of(125L), getAllTsFromDefaultColumn("foo"));
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
        assertEquals(4, results.getStaleValuesDeleted());
        assertThat(results.getCellTsPairsExamined()).isGreaterThanOrEqualTo(5);
        assertEquals("buzz", getFromDefaultColumn("foo", 200));
        assertEquals("", getFromDefaultColumn("foo", 124));
        assertEquals(ImmutableSet.of(-1L, 125L), getAllTsFromDefaultColumn("foo"));
    }

    @Test(timeout = 50000)
    public void testSweepUncommittedThorough() {
        createTable(SweepStrategy.THOROUGH);
        putIntoDefaultColumn("foo", "bar", 50);
        putUncommitted("foo", "baz", 100);
        SweepResults results = completeSweep(175).get();
        assertEquals(1, results.getStaleValuesDeleted());
        assertThat(results.getCellTsPairsExamined()).isGreaterThanOrEqualTo(2);
        assertEquals("bar", getFromDefaultColumn("foo", 750));
        assertEquals(ImmutableSet.of(50L), getAllTsFromDefaultColumn("foo"));
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
        assertEquals(SweepResults.createEmptySweepResult(Optional.empty()), results);
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
        assertEquals(4, results.getStaleValuesDeleted());
        assertThat(results.getCellTsPairsExamined()).isGreaterThanOrEqualTo(6);
        // this check is a nuance of SweepTaskRunner: the value at timestamp 125 is actually eligible for deletion,
        // but we don't delete it on the first pass due to the later uncommitted value. below we sweep again and make
        // sure it's deleted
        assertEquals("", getFromDefaultColumn("foo", 200));
        assertEquals(ImmutableSet.of(125L), getAllTsFromDefaultColumn("foo"));

        results = completeSweep(175).get();
        assertEquals(1, results.getStaleValuesDeleted());
        assertThat(results.getCellTsPairsExamined()).isGreaterThanOrEqualTo(2);
        assertNull(getFromDefaultColumn("foo", 200));
        assertEquals(ImmutableSet.of(), getAllTsFromDefaultColumn("foo"));
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
        assertEquals(4, results.getStaleValuesDeleted());
        assertThat(results.getCellTsPairsExamined()).isGreaterThanOrEqualTo(5);
        assertEquals("foo", getFromDefaultColumn("foo", 200));
        assertEquals(ImmutableSet.of(125L), getAllTsFromDefaultColumn("foo"));
    }

    @SuppressWarnings("unchecked")
    private List<List<Cell>> runSweep(CellsSweeper cellsSweeper, SweepTaskRunner spiedSweepRunner,
            int maxCellTsPairsToExamine, int candidateBatchSize, int deleteBatchSize) {
        sweepTimestamp.set(Long.MAX_VALUE);
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

    protected Optional<SweepResults> completeSweep(long ts) {
        return completeSweep(TABLE_NAME, ts);
    }

    protected Optional<SweepResults> completeSweep(TableReference tableReference, long ts) {
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
            assertEquals(ts, results.getMinSweptTimestamp());
            assertArrayEquals(startRow, results.getPreviousStartRow().orElse(null));
            totalStaleValuesDeleted += results.getStaleValuesDeleted();
            totalCellsExamined += results.getCellTsPairsExamined();
            if (!results.getNextStartRow().isPresent()) {
                return Optional.of(ImmutableSweepResults.builder()
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
                PtBytes.EMPTY_BYTE_ARRAY);
        assertTrue(results.getNextStartRow().isPresent());
        return results;
    }
}

