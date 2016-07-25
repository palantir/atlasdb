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
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Set;

import org.junit.Test;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.cleaner.Follower;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy;
import com.palantir.atlasdb.transaction.api.Transaction;

public class SweepTaskRunnerImplTest {
    private static final TableReference TABLE_REFERENCE = TableReference.create(Namespace.create("ns"), "testTable");
    private static final long VALID_TIMESTAMP = 123L;
    private static final Cell SINGLE_CELL = Cell.create("cellRow".getBytes(), "cellCol".getBytes());
    private static final Set<Cell> SINGLE_CELL_SET = ImmutableSet.of(SINGLE_CELL);
    private static final Multimap<Cell, Long> SINGLE_CELL_TS_PAIR = ImmutableMultimap.<Cell, Long>builder()
            .putAll(Cell.create("cellPairRow".getBytes(), "cellPairCol".getBytes()), ImmutableSet.of(5L, 10L, 15L, 20L))
            .build();

    private final KeyValueService mockKVS = mock(KeyValueService.class);
    private final Follower mockFollower = mock(Follower.class);
    private final Supplier<Long> mockImmutableTimestampSupplier = mock(Supplier.class);
    private final Supplier<Long> mockUnreadableTimestampSupplier = mock(Supplier.class);
    private final SweepTaskRunnerImpl sweepTaskRunner = new SweepTaskRunnerImpl(
            null,
            mockKVS,
            mockUnreadableTimestampSupplier,
            mockImmutableTimestampSupplier,
            null,
            null,
            ImmutableList.of(mockFollower));

    @Test
    public void ensureCellSweepDeletesCells() {
        sweepTaskRunner.sweepCells(TABLE_REFERENCE, SINGLE_CELL_TS_PAIR, ImmutableSet.of());

        verify(mockKVS).delete(TABLE_REFERENCE, SINGLE_CELL_TS_PAIR);
    }

    @Test
    public void ensureSentinelsAreAddedToKVS() {
        sweepTaskRunner.sweepCells(TABLE_REFERENCE, SINGLE_CELL_TS_PAIR, SINGLE_CELL_SET);

        verify(mockKVS).addGarbageCollectionSentinelValues(TABLE_REFERENCE, SINGLE_CELL_SET);
    }

    @Test
    public void ensureFollowersRunAgainstCellsToSweep() {
        sweepTaskRunner.sweepCells(TABLE_REFERENCE, SINGLE_CELL_TS_PAIR, ImmutableSet.of());

        verify(mockFollower).run(any(), any(), eq(SINGLE_CELL_TS_PAIR.keySet()), eq(Transaction.TransactionType.HARD_DELETE));
    }

    @Test
    public void sentinelsArentAddedIfNoCellsToSweep() {
        sweepTaskRunner.sweepCells(TABLE_REFERENCE, ImmutableMultimap.of(), SINGLE_CELL_SET);

        verify(mockKVS, never()).addGarbageCollectionSentinelValues(TABLE_REFERENCE, SINGLE_CELL_SET);
    }

    @Test
    public void ensureNoActionTakenIfNoCellsToSweep() {
        sweepTaskRunner.sweepCells(TABLE_REFERENCE, ImmutableMultimap.of(), ImmutableSet.of());

        verify(mockKVS, never()).delete(any(), any());
        verify(mockKVS, never()).addGarbageCollectionSentinelValues(any(), any());
    }

    @Test
    public void getTimestampsFromEmptyRowResultsReturnsEmptyInThoroughSweep() {
        Multimap<Cell, Long> actualTimestamps = SweepTaskRunnerImpl.getTimestampsFromRowResults(ImmutableList.of(), SweepStrategy.THOROUGH);

        assertThat(actualTimestamps).isEqualTo(ImmutableMultimap.of());
    }

    @Test
    public void getTimestampsFromEmptyRowResultsReturnsEmptyInConservativeSweep() {
        Multimap<Cell, Long> actualTimestamps = SweepTaskRunnerImpl.getTimestampsFromRowResults(ImmutableList.of(), SweepStrategy.CONSERVATIVE);

        assertThat(actualTimestamps).isEqualTo(ImmutableMultimap.of());
    }

    @Test
    public void invalidTimestampsAreFilteredOutWhenGettingTimestampsFromRowResultsInConservativeSweep() {
        List<RowResult<Set<Long>>> cellsToSweep = ImmutableList.of(RowResult.of(SINGLE_CELL, ImmutableSet.of(Value.INVALID_VALUE_TIMESTAMP)));

        Multimap<Cell, Long> actualTimestamps = SweepTaskRunnerImpl.getTimestampsFromRowResults(cellsToSweep, SweepStrategy.CONSERVATIVE);

        assertThat(actualTimestamps).isEqualTo(ImmutableMultimap.of());
    }

    @Test
    public void invalidTimetampsAreKeptWhenGettingTimestampsFromRowResultsInThoroughSweep() {
        List<RowResult<Set<Long>>> cellsToSweep = ImmutableList.of(RowResult.of(SINGLE_CELL, ImmutableSet.of(Value.INVALID_VALUE_TIMESTAMP)));
        Multimap<Cell, Long> expectedTimestamps = ImmutableMultimap.of(SINGLE_CELL, Value.INVALID_VALUE_TIMESTAMP);

        Multimap<Cell, Long> actualTimestamps = SweepTaskRunnerImpl.getTimestampsFromRowResults(cellsToSweep, SweepStrategy.THOROUGH);

        assertThat(actualTimestamps).isEqualTo(expectedTimestamps);
    }

    @Test
    public void validTimestampsAreKeptWhenGettingTimestampsFromRowResultsInThoroughSweep() {
        List<RowResult<Set<Long>>> cellsToSweep = ImmutableList.of(RowResult.of(SINGLE_CELL, ImmutableSet.of(VALID_TIMESTAMP)));
        Multimap<Cell, Long> expectedTimestamps = ImmutableMultimap.of(SINGLE_CELL, VALID_TIMESTAMP);

        Multimap<Cell, Long> actualTimestamps = SweepTaskRunnerImpl.getTimestampsFromRowResults(cellsToSweep, SweepStrategy.THOROUGH);

        assertThat(actualTimestamps).isEqualTo(expectedTimestamps);
    }

    @Test
    public void validTimestampsAreKeptWhenGettingTimestampsFromRowResultsInConservativeSweep() {
        List<RowResult<Set<Long>>> cellsToSweep = ImmutableList.of(RowResult.of(SINGLE_CELL, ImmutableSet.of(VALID_TIMESTAMP)));
        Multimap<Cell, Long> expectedTimestamps = ImmutableMultimap.of(SINGLE_CELL, VALID_TIMESTAMP);

        Multimap<Cell, Long> actualTimestamps = SweepTaskRunnerImpl.getTimestampsFromRowResults(cellsToSweep, SweepStrategy.CONSERVATIVE);

        assertThat(actualTimestamps).isEqualTo(expectedTimestamps);
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
}
