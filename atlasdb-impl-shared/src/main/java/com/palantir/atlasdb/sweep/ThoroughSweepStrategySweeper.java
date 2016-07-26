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

public class ThoroughSweepStrategySweeper implements SweepStrategySweeper {
    private final KeyValueService keyValueService;
    private final Supplier<Long> immutableTimestampSupplier;

    public ThoroughSweepStrategySweeper(KeyValueService keyValueService, Supplier<Long> immutableTimestampSupplier) {
        this.keyValueService = keyValueService;
        this.immutableTimestampSupplier = immutableTimestampSupplier;
    }

    @Override
    public long getSweepTimestamp() {
        return immutableTimestampSupplier.get();
    }

    @Override
    public ClosableIterator<RowResult<Value>> getValues(TableReference tableReference, RangeRequest rangeRequest, long timestamp) {
        return keyValueService.getRange(tableReference, rangeRequest, timestamp);
    }

    @Override
    public ClosableIterator<RowResult<Set<Long>>> getCellTimestamps(TableReference tableReference, RangeRequest rangeRequest, long timestamp) {
        return keyValueService.getRangeOfTimestamps(tableReference, rangeRequest, timestamp);
    }

    @Override
    public Set<Long> getTimestampsToIgnore() {
        return ImmutableSet.of();
    }

    @Override
    public boolean shouldAddSentinels() {
        return false;
    }
}
