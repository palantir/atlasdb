package com.palantir.atlasdb.keyvalue.partition.api;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.Multimap;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;

public interface TableAwarePartitionMapApi {

    Set<KeyValueService> getServicesForRead(String tableName, byte[] row);
    Set<KeyValueService> getServicesForWrite(String tableName, byte[] row);

    Multimap<RangeRequest, KeyValueService> getServicesForRangeRead(String tableName, RangeRequest range);
    Multimap<RangeRequest, KeyValueService> getServicesForRangeWrite(String tableName, RangeRequest range);

    void insertServices(String tableName, Set<KeyValueService> kvs);
    void removeServices(String tableName, Set<KeyValueService> kvs);

    Set<String> getAllTableNames();

    void addTable(String tableName, int maxValueSize) throws InsufficientConsistencyException;
    void dropTable(String tableName) throws InsufficientConsistencyException;
    void truncateTable(String tableName) throws InsufficientConsistencyException;
    void truncateTables(Set<String> tableNamess) throws InsufficientConsistencyException;

    void storeTableMetadata(String tableName, byte[] metadata);
    byte[] getTableMetadata(String tableName);
    Map<String, byte[]> getTablesMetadata();

    void tearDown();
    void close();

}
