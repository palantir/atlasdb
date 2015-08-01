package com.palantir.atlasdb.keyvalue.partition;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.common.annotation.Idempotent;
import com.palantir.common.annotation.NonIdempotent;
import com.palantir.common.base.ClosableIterator;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

public class FailableKeyValueService implements KeyValueService {

    final KeyValueService backingKvs;
    boolean broken;

    private FailableKeyValueService(KeyValueService backingKvs) {
        this.backingKvs = backingKvs;
        broken = false;
    }

    public static FailableKeyValueService wrap(KeyValueService kvs) {
        return new FailableKeyValueService(kvs);
    }

    public void stop() {
        broken = true;
    }

    public void start() {
        broken = false;
    }

    private void validateNotBroken() {
        Preconditions.checkState(broken == false);
    }

    @Override
    public void initializeFromFreshInstance() {
        validateNotBroken();
        backingKvs.initializeFromFreshInstance();
    }

    @Override
    public void close() {
        validateNotBroken();
        backingKvs.close();
    }

    @Override
    public void teardown() {
        validateNotBroken();
        backingKvs.teardown();
    }

    @Override
    public Collection<? extends KeyValueService> getDelegates() {
        validateNotBroken();
        return backingKvs.getDelegates();
    }

    @Override
    @Idempotent
    public Map<Cell, Value> getRows(String tableName,
                             Iterable<byte[]> rows,
                             ColumnSelection columnSelection,
                             long timestamp) {
        validateNotBroken();
        return backingKvs.getRows(tableName, rows, columnSelection, timestamp);
    }

    @Override
    @Idempotent
    public Map<Cell, Value> get(String tableName, Map<Cell, Long> timestampByCell) {
        validateNotBroken();
        return backingKvs.get(tableName, timestampByCell);
    }

    @Override
    @Idempotent
    public Map<Cell, Long> getLatestTimestamps(String tableName, Map<Cell, Long> timestampByCell) {
        validateNotBroken();
        return backingKvs.getLatestTimestamps(tableName, timestampByCell);
    }

    @Override
    public void put(String tableName, Map<Cell, byte[]> values, long timestamp)
            throws KeyAlreadyExistsException {
        validateNotBroken();
        backingKvs.put(tableName, values, timestamp);

    }

    @Override
    public void multiPut(Map<String, ? extends Map<Cell, byte[]>> valuesByTable, long timestamp)
            throws KeyAlreadyExistsException {
        validateNotBroken();
        backingKvs.multiPut(valuesByTable, timestamp);
    }

    @Override
    @NonIdempotent
    public void putWithTimestamps(String tableName, Multimap<Cell, Value> cellValues)
            throws KeyAlreadyExistsException {
        validateNotBroken();
        backingKvs.putWithTimestamps(tableName, cellValues);
    }

    @Override
    public void putUnlessExists(String tableName, Map<Cell, byte[]> values)
            throws KeyAlreadyExistsException {
        validateNotBroken();
        backingKvs.putUnlessExists(tableName, values);
    }

    @Override
    @Idempotent
    public void delete(String tableName, Multimap<Cell, Long> keys) {
        validateNotBroken();
        backingKvs.delete(tableName, keys);
    }

    @Override
    @Idempotent
    public void truncateTable(String tableName) throws InsufficientConsistencyException {
        validateNotBroken();
        backingKvs.truncateTable(tableName);
    }

    @Override
    @Idempotent
    public void truncateTables(Set<String> tableNames) throws InsufficientConsistencyException {
        validateNotBroken();
        backingKvs.truncateTables(tableNames);
    }

    @Override
    @Idempotent
    public ClosableIterator<RowResult<Value>> getRange(String tableName,
                                                RangeRequest rangeRequest,
                                                long timestamp) {
        validateNotBroken();
        return backingKvs.getRange(tableName, rangeRequest, timestamp);
    }

    @Override
    @Idempotent
    public ClosableIterator<RowResult<Set<Value>>> getRangeWithHistory(String tableName,
                                                                RangeRequest rangeRequest,
                                                                long timestamp) {
        validateNotBroken();
        return backingKvs.getRangeWithHistory(tableName, rangeRequest, timestamp);
    }

    @Override
    @Idempotent
    public ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(String tableName,
                                                                RangeRequest rangeRequest,
                                                                long timestamp)
            throws InsufficientConsistencyException {
        validateNotBroken();
        return backingKvs.getRangeOfTimestamps(tableName, rangeRequest, timestamp);
    }

    @Override
    @Idempotent
    public Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(String tableName,
                                                                                                    Iterable<RangeRequest> rangeRequests,
                                                                                                    long timestamp) {
        validateNotBroken();
        return backingKvs.getFirstBatchForRanges(tableName, rangeRequests, timestamp);
    }

    @Override
    @Idempotent
    public void dropTable(String tableName) throws InsufficientConsistencyException {
        validateNotBroken();
        backingKvs.dropTable(tableName);
    }

    @Override
    @Idempotent
    public void createTable(String tableName, int maxValueSizeInBytes)
            throws InsufficientConsistencyException {
//        validateNotBroken();
        backingKvs.createTable(tableName, maxValueSizeInBytes);
    }

    @Override
    @Idempotent
    public void createTables(Map<String, Integer> tableNamesToMaxValueSizeInBytes)
            throws InsufficientConsistencyException {
        validateNotBroken();
        backingKvs.createTables(tableNamesToMaxValueSizeInBytes);
    }

    @Override
    @Idempotent
    public Set<String> getAllTableNames() {
        validateNotBroken();
        return backingKvs.getAllTableNames();
    }

    @Override
    @Idempotent
    public byte[] getMetadataForTable(String tableName) {
        validateNotBroken();
        return backingKvs.getMetadataForTable(tableName);
    }

    @Override
    @Idempotent
    public Map<String, byte[]> getMetadataForTables() {
        validateNotBroken();
        return backingKvs.getMetadataForTables();
    }

    @Override
    @Idempotent
    public void putMetadataForTable(String tableName, byte[] metadata) {
        validateNotBroken();
        backingKvs.putMetadataForTable(tableName, metadata);
    }

    @Override
    @Idempotent
    public void putMetadataForTables(Map<String, byte[]> tableNameToMetadata) {
        validateNotBroken();
        backingKvs.putMetadataForTables(tableNameToMetadata);
    }

    @Override
    @Idempotent
    public void addGarbageCollectionSentinelValues(String tableName, Set<Cell> cells) {
        validateNotBroken();
        backingKvs.addGarbageCollectionSentinelValues(tableName, cells);
    }

    @Override
    @Idempotent
    public Multimap<Cell, Long> getAllTimestamps(String tableName, Set<Cell> cells, long timestamp)
            throws InsufficientConsistencyException {
        validateNotBroken();
        return backingKvs.getAllTimestamps(tableName, cells, timestamp);
    }

    @Override
    public void compactInternally(String tableName) {
        validateNotBroken();
        backingKvs.compactInternally(tableName);
    }

}
