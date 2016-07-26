package com.palantir.atlasdb.sweep;

import java.util.Set;

import org.immutables.value.Value;

import com.google.common.collect.Multimap;
import com.palantir.atlasdb.keyvalue.api.Cell;

@Value.Immutable
public abstract class SweepCellsAndSentinels {

    public abstract Multimap<Cell, Long> startTimestampsToSweepPerCell();
    public abstract Set<Cell> sentinelsToAdd();

    public static SweepCellsAndSentinels of(Multimap<Cell, Long> startTimestampsToSweepPerCell, Set<Cell> sentinelsToAdd) {
        return ImmutableSweepCellsAndSentinels.builder()
                .startTimestampsToSweepPerCell(startTimestampsToSweepPerCell)
                .sentinelsToAdd(sentinelsToAdd)
                .build();
    }
}
