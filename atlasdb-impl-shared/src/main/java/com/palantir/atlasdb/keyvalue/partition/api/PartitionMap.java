package com.palantir.atlasdb.keyvalue.partition.api;

import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;

import com.google.common.collect.Multimap;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.partition.ConsistentRingRangeRequest;

public interface PartitionMap {

    Multimap<ConsistentRingRangeRequest, KeyValueService> getServicesForRangeRead(String tableName, RangeRequest range);

    Map<KeyValueService, NavigableSet<byte[]>> getServicesForRowsRead(String tableName, Iterable<byte[]> rows);

    Map<KeyValueService, Map<Cell, Long>> getServicesForCellsRead(String tableName, Map<Cell, Long> timestampByCell);
    Map<KeyValueService, Set<Cell>> getServicesForCellsRead(String tableName, Set<Cell> cells);

    Map<KeyValueService, Map<Cell, byte[]>> getServicesForCellsWrite(String tableName,
                                                                     Map<Cell, byte[]> values);
    Map<KeyValueService, Set<Cell>> getServicesForCellsWrite(String tableName,
                                                                     Set<Cell> cells);

    <T> Map<KeyValueService, Multimap<Cell, T>> getServicesForWrite(String tableName, Multimap<Cell, T> keys);

    Set<? extends KeyValueService> getDelegates();

}
