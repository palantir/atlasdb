package com.palantir.atlasdb.sweep;

import java.util.Set;

import com.google.common.collect.Multimap;
import com.palantir.atlasdb.keyvalue.api.Cell;

public class SweepCellsAndSentinels {

    private final Multimap<Cell, Long> startTimestampsToSweepPerCell;
    private Set<Cell> sentinelsToAdd;

    public SweepCellsAndSentinels(Multimap<Cell, Long> startTimestampsToSweepPerCell, Set<Cell> sentinelsToAdd) {
        this.startTimestampsToSweepPerCell = startTimestampsToSweepPerCell;
        this.sentinelsToAdd = sentinelsToAdd;
    }

    public Multimap<Cell, Long> getStartTimestampsToSweepPerCell() {
        return startTimestampsToSweepPerCell;
    }

    public Set<Cell> getSentinelsToAdd() {
        return sentinelsToAdd;
    }
}
