package com.palantir.atlasdb.keyvalue.dbkvs.impl;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.Value;

public interface DbWriteTable {
    void put(Collection<Map.Entry<Cell, byte[]>> data, long ts);
    void put(Collection<Map.Entry<Cell, Value>> data);
    void putSentinels(Iterable<Cell> cells);
    void delete(List<Entry<Cell, Long>> partition);
}
