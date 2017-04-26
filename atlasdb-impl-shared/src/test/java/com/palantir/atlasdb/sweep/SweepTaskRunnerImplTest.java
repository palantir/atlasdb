/*
 * Copyright 2016 Palantir Technologies
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
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyCollection;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anySet;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.LongSupplier;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Multimap;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManager;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.common.base.ClosableIterators;

public class SweepTaskRunnerImplTest {
    private static final long LOW_START_TS = 6L;
    private static final long LOW_COMMIT_TS = 7L;
    private static final long HIGH_START_TS = 100L;
    private static final long HIGH_COMMIT_TS = 102L;
    private static final long VALID_TIMESTAMP = 123L;

    private static final TableReference TABLE_REFERENCE = TableReference.create(Namespace.create("ns"), "testTable");
    private static final Cell SINGLE_CELL = Cell.create(
            "cellRow".getBytes(StandardCharsets.UTF_8),
            "cellCol".getBytes(StandardCharsets.UTF_8));

    public static final long OLD_START_TS = 10L;
    public static final long COMMIT_TS = 30L;
    public static final long NEW_START_TS = 20L;
    public static final int ROW_BATCH_SIZE = 1000;

    private final KeyValueService mockKvs = mock(KeyValueService.class);
    private final LongSupplier mockImmutableTimestampSupplier = mock(LongSupplier.class);
    private final LongSupplier mockUnreadableTimestampSupplier = mock(LongSupplier.class);
    private final TransactionService mockTransactionService = mock(TransactionService.class);
    private final CellsSweeper mockCellsSweeper = mock(CellsSweeper.class);
    private final SweepStrategyManager mockSweepStrategyManager = mock(SweepStrategyManager.class);
    private final SweepTaskRunner sweepTaskRunner = new SweepTaskRunner(
            mockKvs,
            mockUnreadableTimestampSupplier,
            mockImmutableTimestampSupplier,
            mockTransactionService,
            mockSweepStrategyManager,
            mockCellsSweeper);

    private static CellsAndTimestamps convertToCellAndTimestamps(Multimap<Cell, Long> multimap) {
        ImmutableCellsAndTimestamps.Builder builder = ImmutableCellsAndTimestamps.builder();
        for (Cell cell : multimap.keySet()) {
            builder.addCellAndTimestampsList(CellAndTimestamps.of(cell, ImmutableSet.copyOf(multimap.get(cell))));
        }
        return builder.build();
    }

    @Test
    public void getTimestampsToSweep_noRowsMeansNoTransactionGets() {
        sweepTaskRunner.getStartTimestampsPerRowToSweep(
                convertToCellAndTimestamps(ImmutableMultimap.of()),
                Iterators.peekingIterator(Collections.emptyIterator()),
                VALID_TIMESTAMP,
                Sweeper.CONSERVATIVE);

        verify(mockTransactionService, never()).get(any());
        verify(mockTransactionService, never()).get(anyLong());
    }

    @Test
    public void conservative_getTimestampsToSweep_twoEntriesBelowSweepTimestamp_returnsLowerOne() {
        long sweepTimestampHigherThanCommitTimestamp = HIGH_COMMIT_TS + 1;
        Multimap<Cell, Long> timestampsPerRow = twoCommittedTimestampsForSingleCell();

        Multimap<Cell, Long> startTimestampsPerRowToSweep = sweepTaskRunner.getStartTimestampsPerRowToSweep(
                convertToCellAndTimestamps(timestampsPerRow),
                Iterators.peekingIterator(Collections.emptyIterator()),
                sweepTimestampHigherThanCommitTimestamp,
                Sweeper.CONSERVATIVE).timestampsAsMultimap();

        assertThat(startTimestampsPerRowToSweep.get(SINGLE_CELL)).contains(LOW_START_TS);
    }

    @Test
    public void conservative_getTimestampsToSweep_oneEntryBelowTimestamp_oneAbove_returnsNone() {
        long sweepTimestampLowerThanCommitTimestamp = HIGH_COMMIT_TS - 1;
        Multimap<Cell, Long> timestampsPerRow = twoCommittedTimestampsForSingleCell();

        Multimap<Cell, Long> startTimestampsPerRowToSweep = sweepTaskRunner.getStartTimestampsPerRowToSweep(
                convertToCellAndTimestamps(timestampsPerRow),
                Iterators.peekingIterator(Collections.emptyIterator()),
                sweepTimestampLowerThanCommitTimestamp,
                Sweeper.CONSERVATIVE).timestampsAsMultimap();

        assertThat(startTimestampsPerRowToSweep.get(SINGLE_CELL)).isEmpty();
    }

    @Test
    public void conservativeGetTimestampToSweepAddsSentinels() {
        long sweepTimestampHigherThanCommitTimestamp = HIGH_COMMIT_TS + 1;
        Multimap<Cell, Long> timestampsPerRow = twoCommittedTimestampsForSingleCell();

        CellsToSweep cellsToSweep = sweepTaskRunner.getStartTimestampsPerRowToSweep(
                convertToCellAndTimestamps(timestampsPerRow),
                Iterators.peekingIterator(ClosableIterators.emptyImmutableClosableIterator()),
                sweepTimestampHigherThanCommitTimestamp,
                Sweeper.CONSERVATIVE);

        assertThat(cellsToSweep.allSentinels()).contains(SINGLE_CELL);
    }

    @Test
    public void thoroughGetTimestampToSweepDoesNotAddSentinels() {
        long sweepTimestampHigherThanCommitTimestamp = HIGH_COMMIT_TS + 1;
        Multimap<Cell, Long> timestampsPerRow = twoCommittedTimestampsForSingleCell();

        CellsToSweep cellsToSweep = sweepTaskRunner.getStartTimestampsPerRowToSweep(
                convertToCellAndTimestamps(timestampsPerRow),
                Iterators.peekingIterator(ClosableIterators.emptyImmutableClosableIterator()),
                sweepTimestampHigherThanCommitTimestamp,
                Sweeper.THOROUGH);

        assertThat(cellsToSweep.allSentinels()).isEmpty();
    }

    @Test
    public void getTimestampsToSweep_onlyTransactionUncommitted_returnsIt() {
        Multimap<Cell, Long> timestampsPerRow = ImmutableMultimap.of(SINGLE_CELL, LOW_START_TS);
        when(mockTransactionService.get(anyCollection()))
                .thenReturn(ImmutableMap.of(LOW_START_TS, TransactionConstants.FAILED_COMMIT_TS));

        CellsToSweep cellsToSweep = sweepTaskRunner.getStartTimestampsPerRowToSweep(
                convertToCellAndTimestamps(timestampsPerRow),
                Iterators.peekingIterator(ClosableIterators.emptyImmutableClosableIterator()),
                HIGH_START_TS,
                Sweeper.CONSERVATIVE);
        Multimap<Cell, Long> timestampsToSweep = cellsToSweep.timestampsAsMultimap();

        assertThat(timestampsToSweep.get(SINGLE_CELL)).contains(LOW_START_TS);
    }

    @Test
    public void thorough_getTimestampsToSweep_oneTransaction_emptyValue_returnsIt() {
        Multimap<Cell, Long> timestampsPerRow = ImmutableMultimap.of(SINGLE_CELL, LOW_START_TS);
        when(mockTransactionService.get(anyCollection()))
                .thenReturn(ImmutableMap.of(LOW_START_TS, LOW_COMMIT_TS));
        RowResult<Value> rowResult = RowResult.of(SINGLE_CELL, Value.create(null, LOW_START_TS));

        CellsToSweep cellsToSweep = sweepTaskRunner.getStartTimestampsPerRowToSweep(
                convertToCellAndTimestamps(timestampsPerRow),
                Iterators.peekingIterator(ClosableIterators.wrap(ImmutableList.of(rowResult).iterator())),
                HIGH_START_TS,
                Sweeper.THOROUGH);
        Multimap<Cell, Long> timestampsToSweep = cellsToSweep.timestampsAsMultimap();

        assertThat(timestampsToSweep.get(SINGLE_CELL)).contains(LOW_START_TS);
    }

    @Test
    public void cellSweeperCallsShouldBatchRows() {
        int numBatches = 3;
        int maxBatchSize = 100;
        int numRows = numBatches * maxBatchSize;
        List<RowResult<Set<Long>>> timestamps = makeTimestamps(numRows, 1);

        verifyBatching(numBatches, maxBatchSize, timestamps);
    }

    @Test
    public void cellSweeperCallsShouldBatchColumns() {
        int numBatches = 3;
        int maxBatchSize = 100;
        int numCols = numBatches * maxBatchSize;
        List<RowResult<Set<Long>>> timestamps = makeTimestamps(1, numCols);

        verifyBatching(numBatches, maxBatchSize, timestamps);
    }

    private void verifyBatching(int batches, int maxBatchSize, List<RowResult<Set<Long>>> timestamps) {
        ImmutableMap<Long, Long> startToCommitTs = ImmutableMap.of(OLD_START_TS, COMMIT_TS, NEW_START_TS, COMMIT_TS);
        when(mockTransactionService.get(any(Iterable.class))).thenReturn(startToCommitTs);
        when(mockKvs.getMetadataForTable(TABLE_REFERENCE)).thenReturn(new byte[1]);
        when(mockSweepStrategyManager.get()).thenReturn(ImmutableMap.of());
        when(mockImmutableTimestampSupplier.getAsLong()).thenReturn(100L);
        when(mockUnreadableTimestampSupplier.getAsLong()).thenReturn(100L);
        when(mockKvs.getRangeOfTimestamps(eq(TABLE_REFERENCE), any(RangeRequest.class), anyLong()))
                .thenReturn(ClosableIterators.wrap(timestamps.iterator()));

        sweepTaskRunner.run(TABLE_REFERENCE, ROW_BATCH_SIZE, maxBatchSize, null);

        verifyAllCallsRespectBatching(batches, maxBatchSize);
    }

    private void verifyAllCallsRespectBatching(int batches, int maxBatchSize) {
        ArgumentCaptor<Multimap> captor = ArgumentCaptor.forClass(Multimap.class);
        verify(mockCellsSweeper, times(batches)).sweepCells(eq(TABLE_REFERENCE), captor.capture(), anySet());

        for (Multimap capturedMap : captor.getAllValues()) {
            assertThat(capturedMap.size()).isLessThanOrEqualTo(maxBatchSize);
        }
    }

    private List<RowResult<Set<Long>>> makeTimestamps(int rows, int cols) {
        ImmutableList.Builder<RowResult<Set<Long>>> rowsBuilder = ImmutableList.builder();
        Set<Long> timestampsSet = ImmutableSet.of(OLD_START_TS, NEW_START_TS);
        for (int row = 0; row < rows; row++) {
            SortedMap<byte[], Set<Long>> columns =
                    new ConcurrentSkipListMap<>(UnsignedBytes.lexicographicalComparator());
            for (int col = 0; col < cols; col++) {
                byte[] colName = ("col" + col).getBytes(StandardCharsets.UTF_8);
                columns.put(colName, timestampsSet);
            }

            byte[] rowName = ("row" + row).getBytes(StandardCharsets.UTF_8);
            rowsBuilder.add(RowResult.create(rowName, columns));
        }
        return rowsBuilder.build();
    }

    private Multimap<Cell, Long> twoCommittedTimestampsForSingleCell() {
        Multimap<Cell, Long> timestampsPerRow = ImmutableMultimap.of(
                SINGLE_CELL, LOW_START_TS,
                SINGLE_CELL, HIGH_START_TS);

        when(mockTransactionService.get(anyCollection()))
                .thenReturn(ImmutableMap.of(
                        LOW_START_TS, LOW_COMMIT_TS,
                        HIGH_START_TS, HIGH_COMMIT_TS));

        return timestampsPerRow;
    }
}
