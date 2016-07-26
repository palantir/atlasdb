package com.palantir.atlasdb.sweep;

import java.util.Set;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ClosableIterators;

public class ConservativeSweepStrategySweeper implements SweepStrategySweeper {
    private final KeyValueService keyValueService;
    private final Supplier<Long> immutableTimestampSupplier;
    private final Supplier<Long> unreadableTimestampSupplier;

    public ConservativeSweepStrategySweeper(KeyValueService keyValueService, Supplier<Long> immutableTimestampSupplier, Supplier<Long> unreadableTimestampSupplier) {
        this.keyValueService = keyValueService;
        this.immutableTimestampSupplier = immutableTimestampSupplier;
        this.unreadableTimestampSupplier = unreadableTimestampSupplier;
    }

    @Override
    public long getSweepTimestamp() {
        return Math.min(unreadableTimestampSupplier.get(), immutableTimestampSupplier.get());
    }

    @Override
    public ClosableIterator<RowResult<Value>> getValues(TableReference tableReference, RangeRequest rangeRequest, long timestamp) {
        return ClosableIterators.emptyImmutableClosableIterator();
    }

    @Override
    public ClosableIterator<RowResult<Set<Long>>> getCellTimestamps(TableReference tableReference, RangeRequest rangeRequest, long timestamp) {
        return keyValueService.getRangeOfTimestamps(tableReference, rangeRequest, timestamp);
    }

    @Override
    public Set<Long> getTimestampsToIgnore() {
        return ImmutableSet.of(Value.INVALID_VALUE_TIMESTAMP);
    }

    @Override
    public boolean shouldAddSentinels() {
        return true;
    }
}
