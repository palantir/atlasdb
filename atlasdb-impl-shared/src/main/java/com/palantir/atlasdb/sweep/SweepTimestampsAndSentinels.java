package com.palantir.atlasdb.sweep;

import java.util.Set;

import com.palantir.atlasdb.keyvalue.api.Cell;

public class SweepTimestampsAndSentinels {
    private final Set<Long> timestamps;
    private final Set<Cell> sentinelsToAdd;

    public SweepTimestampsAndSentinels(Set<Long> timestamps, Set<Cell> sentinelsToAdd) {
        this.timestamps = timestamps;
        this.sentinelsToAdd = sentinelsToAdd;
    }

    public Set<Cell> getSentinelsToAdd() {
        return sentinelsToAdd;
    }

    public Set<Long> getTimestamps() {
        return timestamps;
    }
}
