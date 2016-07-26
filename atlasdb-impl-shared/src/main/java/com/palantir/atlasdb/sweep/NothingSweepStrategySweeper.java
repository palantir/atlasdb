package com.palantir.atlasdb.sweep;

import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ClosableIterators;

public class NothingSweepStrategySweeper implements SweepStrategySweeper {
    @Override
    public long getSweepTimestamp() {
        return 0;
    }

    @Override
    public ClosableIterator<RowResult<Value>> getValues(TableReference tableReference, RangeRequest rangeRequest, long timestamp) {
        return ClosableIterators.emptyImmutableClosableIterator();
    }

    @Override
    public ClosableIterator<RowResult<Set<Long>>> getCellTimestamps(TableReference tableReference, RangeRequest rangeRequest, long timestamp) {
        return ClosableIterators.emptyImmutableClosableIterator();
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
