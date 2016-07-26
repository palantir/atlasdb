package com.palantir.atlasdb.sweep;

import java.util.Set;

import org.immutables.value.Value;

import com.palantir.atlasdb.keyvalue.api.Cell;

@Value.Immutable
public abstract class SweepTimestampsAndSentinels {
    public abstract Set<Long> timestamps();
    public abstract Set<Cell> sentinelsToAdd();

    public static SweepTimestampsAndSentinels of(Set<Long> timestamps, Set<Cell> sentinelsToAdd) {
        return ImmutableSweepTimestampsAndSentinels.builder()
                .timestamps(timestamps)
                .sentinelsToAdd(sentinelsToAdd)
                .build();
    }
}
