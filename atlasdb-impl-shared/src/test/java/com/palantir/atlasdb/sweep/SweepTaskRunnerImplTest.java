/**
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.Collections;

import org.junit.Test;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy;
import com.palantir.atlasdb.sweep.sweepers.ConservativeSweeper;
import com.palantir.atlasdb.sweep.sweepers.Sweeper;
import com.palantir.atlasdb.sweep.sweepers.ThoroughSweeper;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.common.base.ClosableIterators;

public class SweepTaskRunnerImplTest {
    private static final long LOW_START_TS = 6L;
    private static final long LOW_COMMIT_TS = 7L;
    private static final long HIGH_START_TS = 100L;
    private static final long HIGH_COMMIT_TS = 102L;
    private static final long VALID_TIMESTAMP = 123L;

    private static final Cell SINGLE_CELL = Cell.create(
            "cellRow".getBytes(StandardCharsets.UTF_8),
            "cellCol".getBytes(StandardCharsets.UTF_8));

    private final KeyValueService mockKvs = mock(KeyValueService.class);
    private final Supplier<Long> mockImmutableTimestampSupplier = mock(Supplier.class);
    private final Supplier<Long> mockUnreadableTimestampSupplier = mock(Supplier.class);
    private final TransactionService mockTransactionService = mock(TransactionService.class);
    private final CellsSweeper mockCellsSweeper = mock(CellsSweeper.class);
    private final SweepTaskRunnerImpl sweepTaskRunner = new SweepTaskRunnerImpl(
            mockKvs,
            mockUnreadableTimestampSupplier,
            mockImmutableTimestampSupplier,
            mockTransactionService,
            null,
            mockCellsSweeper);
    private final Sweeper thoroughSweeper = new ThoroughSweeper(mockKvs, mockImmutableTimestampSupplier);
    private final Sweeper conservativeSweeper = new ConservativeSweeper(
            mockKvs,
            mockImmutableTimestampSupplier,
            mockUnreadableTimestampSupplier);

    @Test
    public void sweepValidTimestamps() {
        CellAndTimestamps itemWithValidTimestamp = CellAndTimestamps.of(SINGLE_CELL, ImmutableSet.of(VALID_TIMESTAMP));
        CellsAndTimestamps cellsToSweep = CellsAndTimestamps.withSingleItem(itemWithValidTimestamp);

        CellsAndTimestamps result = SweepTaskRunnerImpl.removeIgnoredTimestamps(cellsToSweep, ImmutableSet.of());

        assertThat(result).isEqualTo(cellsToSweep);
    }

    @Test
    public void doNotSweepIgnoredTimestamps() {
        CellAndTimestamps itemWithNoTimestamps = CellAndTimestamps.of(SINGLE_CELL, ImmutableSet.of());
        CellAndTimestamps itemWithInvalidTimestamp =
                CellAndTimestamps.of(SINGLE_CELL, ImmutableSet.of(Value.INVALID_VALUE_TIMESTAMP));
        CellsAndTimestamps cellsToSweep = CellsAndTimestamps.withSingleItem(itemWithInvalidTimestamp);

        CellsAndTimestamps result = SweepTaskRunnerImpl.removeIgnoredTimestamps(
                cellsToSweep, ImmutableSet.of(Value.INVALID_VALUE_TIMESTAMP));

        assertThat(result).isEqualTo(CellsAndTimestamps.withSingleItem(itemWithNoTimestamps));
    }

    @Test
    public void thoroughWillReturnTheImmutableTimestamp() {
        when(mockImmutableTimestampSupplier.get()).thenReturn(VALID_TIMESTAMP);

        assertThat(sweepTaskRunner.getSweepTimestamp(SweepStrategy.THOROUGH)).isEqualTo(VALID_TIMESTAMP);
    }

    @Test
    public void conservativeWillReturnTheImmutableTimestampIfItIsLowerThanUnreadableTimestamp() {
        when(mockImmutableTimestampSupplier.get()).thenReturn(100L);
        when(mockUnreadableTimestampSupplier.get()).thenReturn(200L);

        assertThat(sweepTaskRunner.getSweepTimestamp(SweepStrategy.CONSERVATIVE)).isEqualTo(100L);
    }

    @Test
    public void conservativeWillReturnTheUnreadableTimestampIfItIsLowerThanImmutableTimestamp() {
        when(mockImmutableTimestampSupplier.get()).thenReturn(200L);
        when(mockUnreadableTimestampSupplier.get()).thenReturn(100L);

        assertThat(sweepTaskRunner.getSweepTimestamp(SweepStrategy.CONSERVATIVE)).isEqualTo(100L);
    }

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
                conservativeSweeper);

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
                conservativeSweeper).timestampsAsMultimap();

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
                conservativeSweeper).timestampsAsMultimap();

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
                conservativeSweeper);

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
                thoroughSweeper);

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
                conservativeSweeper);
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
                thoroughSweeper);
        Multimap<Cell, Long> timestampsToSweep = cellsToSweep.timestampsAsMultimap();

        assertThat(timestampsToSweep.get(SINGLE_CELL)).contains(LOW_START_TS);
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
