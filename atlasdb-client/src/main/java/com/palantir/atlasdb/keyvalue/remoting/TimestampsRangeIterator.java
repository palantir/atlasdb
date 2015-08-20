package com.palantir.atlasdb.keyvalue.remoting;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.common.base.ClosableIterator;

public class TimestampsRangeIterator extends RangeIterator<Set<Long>> {
    @JsonCreator
    public TimestampsRangeIterator(@JsonProperty("tableName") String tableName,
                                   @JsonProperty("range") RangeRequest range,
                                   @JsonProperty("timestamp") long timestamp,
                                   @JsonProperty("hasNext") boolean hasNext,
                                   @JsonProperty("page") ImmutableList<RowResult<Set<Long>>> page) {
        super(tableName, range, timestamp, hasNext, page);
    }

    @Override
    protected ClosableIterator<RowResult<Set<Long>>> getMoreRows(KeyValueService kvs, String tableName,
                                                                 RangeRequest newRange, long timestamp) {
        return kvs.getRangeOfTimestamps(tableName, newRange, timestamp);
    }
}
