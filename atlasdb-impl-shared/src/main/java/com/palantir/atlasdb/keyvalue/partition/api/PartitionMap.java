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
    Map<KeyValueService, Set<Cell>> getServicesForCellsRead(String tableName, Set<Cell> cells);
    <T> Map<KeyValueService, Map<Cell, T>> getServicesForCellsRead(String tableName, Map<Cell, T> timestampByCell);

    Map<KeyValueService, Set<Cell>> getServicesForCellsWrite(String tableName, Set<Cell> cells);
    <T> Map<KeyValueService, Map<Cell, T>> getServicesForCellsWrite(String tableName, Map<Cell, T> cells);
    <T> Map<KeyValueService, Multimap<Cell, T>> getServicesForCellsWrite(String tableName, Multimap<Cell, T> cells);

    Set<? extends KeyValueService> getDelegates();

}
