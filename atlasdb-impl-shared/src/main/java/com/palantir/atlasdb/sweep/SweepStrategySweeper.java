package com.palantir.atlasdb.sweep;

import java.util.Set;

import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.common.base.ClosableIterator;

public interface SweepStrategySweeper {
    long getSweepTimestamp();

    ClosableIterator<RowResult<Value>> getValues(TableReference tableReference, RangeRequest rangeRequest, long timestamp);

    ClosableIterator<RowResult<Set<Long>>> getCellTimestamps(TableReference tableReference, RangeRequest rangeRequest, long timestamp);

    Set<Long> getTimestampsToIgnore();

    boolean shouldAddSentinels();
}
