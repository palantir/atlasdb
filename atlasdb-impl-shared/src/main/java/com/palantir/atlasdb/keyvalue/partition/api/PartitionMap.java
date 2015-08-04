package com.palantir.atlasdb.keyvalue.partition.api;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.Multimap;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.partition.ConsistentRingRangeRequest;

public interface PartitionMap {

    // Tables
    Set<String> getAllTableNames();
    void createTable(String tableName, int maxValueSize) throws InsufficientConsistencyException;
    void dropTable(String tableName) throws InsufficientConsistencyException;
    void truncateTable(String tableName) throws InsufficientConsistencyException;
    void truncateTables(Set<String> tableNamess) throws InsufficientConsistencyException;
    void storeTableMetadata(String tableName, byte[] metadata);
    byte[] getTableMetadata(String tableName);
    Map<String, byte[]> getTablesMetadata();

    // General
    void tearDown();
    void close();

    // Requests
    Multimap<ConsistentRingRangeRequest, KeyValueService> getServicesForRangeRead(String tableName, RangeRequest range);

    Map<KeyValueService, ? extends Iterable<byte[]>> getServicesForRowsRead(String tableName, Iterable<byte[]> rows);

    Map<KeyValueService, Map<Cell, Long>> getServicesForCellsRead(String tableName, Map<Cell, Long> timestampByCell);
    Map<KeyValueService, Set<Cell>> getServicesForCellsRead(String tableName, Set<Cell> cells, long timestamp);

    Map<KeyValueService, Map<Cell, byte[]>> getServicesForCellsWrite(String tableName,
                                                                     Map<Cell, byte[]> values);
    Map<KeyValueService, Set<Cell>> getServicesForCellsWrite(String tableName,
                                                                     Set<Cell> cells);

    Map<KeyValueService, Multimap<Cell, Value>> getServicesForTimestampsWrite(String tableName,
                                                                              Multimap<Cell, Value> cellValues);

    Map<KeyValueService, Multimap<Cell, Long>> getServicesForDelete(String tableName,
                                                                    Multimap<Cell, Long> keys);
    Set<? extends KeyValueService> getDelegates();
    void compactInternally(String tableName);
    void initializeFromFreshInstance();

}
