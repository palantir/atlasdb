package com.palantir.atlasdb.keyvalue.remoting.iterators;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.common.base.ClosableIterator;

public class HistoryRangeIterator extends RangeIterator<Set<Value>> {
    @JsonCreator
    public HistoryRangeIterator(@JsonProperty("tableName") String tableName,
                                @JsonProperty("range") RangeRequest range,
                                @JsonProperty("timestamp") long timestamp,
                                @JsonProperty("hasNext") boolean hasNext,
                                @JsonProperty("page") ImmutableList<RowResult<Set<Value>>> page) {
        super(tableName, range, timestamp, hasNext, page);
    }

    @Override
    protected ClosableIterator<RowResult<Set<Value>>> getMoreRows(KeyValueService kvs, String tableName,
                                                                  RangeRequest newRange, long timestamp) {
        return kvs.getRangeWithHistory(tableName, newRange, timestamp);
    }

}
