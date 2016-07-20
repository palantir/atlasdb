package com.palantir.atlasdb.sweep;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.Set;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.cleaner.Follower;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.Transaction;

public class SweepTaskRunnerImplTest {
    private static final TableReference TABLE_REFERENCE = TableReference.create(Namespace.create("ns"), "testTable");
    private static final Set<Cell> SINGLE_CELL = ImmutableSet.of(Cell.create("cellRow".getBytes(), "cellCol".getBytes()));
    private static final Multimap<Cell, Long> SINGLE_CELL_TS_PAIR = ImmutableSetMultimap.<Cell, Long>builder()
            .putAll(Cell.create("cellPairRow".getBytes(), "cellPairCol".getBytes()), ImmutableSet.of(5L, 10L, 15L, 20L))
            .build();

    private final KeyValueService mockKVS = mock(KeyValueService.class);
    private final Follower mockFollower = mock(Follower.class);
    private final SweepTaskRunnerImpl sweepTaskRunner = new SweepTaskRunnerImpl(null, mockKVS, null, null, null, null, ImmutableList.of(mockFollower));

    @Test
    public void ensureCellSweepDeletesCells() {
        sweepTaskRunner.sweepCells(TABLE_REFERENCE, SINGLE_CELL_TS_PAIR, ImmutableSet.of());

        verify(mockKVS).delete(TABLE_REFERENCE, SINGLE_CELL_TS_PAIR);
    }

    @Test
    public void ensureSentinelsAreAddedToKVS() {
        sweepTaskRunner.sweepCells(TABLE_REFERENCE, SINGLE_CELL_TS_PAIR, SINGLE_CELL);

        verify(mockKVS).addGarbageCollectionSentinelValues(TABLE_REFERENCE, SINGLE_CELL);
    }

    @Test
    public void ensureFollowersRunAgainstCellsToSweep() {
        sweepTaskRunner.sweepCells(TABLE_REFERENCE, SINGLE_CELL_TS_PAIR, ImmutableSet.of());

        verify(mockFollower).run(any(), any(), eq(SINGLE_CELL_TS_PAIR.keySet()), eq(Transaction.TransactionType.HARD_DELETE));
    }

    @Test
    public void sentinelsArentAddedIfNoCellsToSweep() {
        sweepTaskRunner.sweepCells(TABLE_REFERENCE, ImmutableSetMultimap.of(), SINGLE_CELL);

        verify(mockKVS, never()).addGarbageCollectionSentinelValues(TABLE_REFERENCE, SINGLE_CELL);
    }

    @Test
    public void ensureNoActionTakenIfNoCellsToSweep() {
        sweepTaskRunner.sweepCells(TABLE_REFERENCE, ImmutableSetMultimap.of(), ImmutableSet.of());

        verify(mockKVS, never()).delete(any(), any());
        verify(mockKVS, never()).addGarbageCollectionSentinelValues(any(), any());
    }
}
