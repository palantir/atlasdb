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

    public void setBrokenRead(boolean brokenRead) {
        this.brokenRead = brokenRead;
    }

    public void setBrokenWrite(boolean brokenWrite) {
        this.brokenWrite = brokenWrite;
    }

    public void setBrokenMeta(boolean brokenMeta) {
        this.brokenMeta = brokenMeta;
    }

    boolean brokenRead;
    boolean brokenWrite;
    boolean brokenMeta;

    private FailableKeyValueService(KeyValueService backingKvs) {
        this.backingKvs = backingKvs;
        this.brokenRead = false;
        this.brokenWrite = false;
        this.brokenMeta = false;
    }

    public static FailableKeyValueService wrap(KeyValueService kvs) {
        return new FailableKeyValueService(kvs);
    }

    private void enterReadMethod() {
        Preconditions.checkState(!brokenRead);
    }

    private void enterWriteMethod() {
        Preconditions.checkState(!brokenWrite);
    }

    private void enterMetaMethod() {
        Preconditions.checkState(!brokenMeta);
    }

    @Override
    public void initializeFromFreshInstance() {
        enterMetaMethod();
        backingKvs.initializeFromFreshInstance();
    }

    @Override
    public void close() {
        enterMetaMethod();
        backingKvs.close();
    }

    @Override
    public void teardown() {
        enterMetaMethod();
        backingKvs.teardown();
    }

    @Override
    public Collection<? extends KeyValueService> getDelegates() {
        enterMetaMethod();
        return backingKvs.getDelegates();
    }

    @Override
    @Idempotent
    public Map<Cell, Value> getRows(String tableName,
                             Iterable<byte[]> rows,
                             ColumnSelection columnSelection,
                             long timestamp) {
        enterReadMethod();
        return backingKvs.getRows(tableName, rows, columnSelection, timestamp);
    }

    @Override
    @Idempotent
    public Map<Cell, Value> get(String tableName, Map<Cell, Long> timestampByCell) {
        enterReadMethod();
        return backingKvs.get(tableName, timestampByCell);
    }

    @Override
    @Idempotent
    public Map<Cell, Long> getLatestTimestamps(String tableName, Map<Cell, Long> timestampByCell) {
        enterReadMethod();
        return backingKvs.getLatestTimestamps(tableName, timestampByCell);
    }

    @Override
    public void put(String tableName, Map<Cell, byte[]> values, long timestamp)
            throws KeyAlreadyExistsException {
        enterWriteMethod();
        backingKvs.put(tableName, values, timestamp);

    }

    @Override
    public void multiPut(Map<String, ? extends Map<Cell, byte[]>> valuesByTable, long timestamp)
            throws KeyAlreadyExistsException {
        enterWriteMethod();
        backingKvs.multiPut(valuesByTable, timestamp);
    }

    @Override
    @NonIdempotent
    public void putWithTimestamps(String tableName, Multimap<Cell, Value> cellValues)
            throws KeyAlreadyExistsException {
        enterWriteMethod();
        backingKvs.putWithTimestamps(tableName, cellValues);
    }

    @Override
    public void putUnlessExists(String tableName, Map<Cell, byte[]> values)
            throws KeyAlreadyExistsException {
        enterWriteMethod();
        backingKvs.putUnlessExists(tableName, values);
    }

    @Override
    @Idempotent
    public void delete(String tableName, Multimap<Cell, Long> keys) {
        enterWriteMethod();
        backingKvs.delete(tableName, keys);
    }

    @Override
    @Idempotent
    public void truncateTable(String tableName) throws InsufficientConsistencyException {
        enterWriteMethod();
        backingKvs.truncateTable(tableName);
    }

    @Override
    @Idempotent
    public void truncateTables(Set<String> tableNames) throws InsufficientConsistencyException {
        enterWriteMethod();
        backingKvs.truncateTables(tableNames);
    }

    @Override
    @Idempotent
    public ClosableIterator<RowResult<Value>> getRange(String tableName,
                                                RangeRequest rangeRequest,
                                                long timestamp) {
        enterReadMethod();
        return backingKvs.getRange(tableName, rangeRequest, timestamp);
    }

    @Override
    @Idempotent
    public ClosableIterator<RowResult<Set<Value>>> getRangeWithHistory(String tableName,
                                                                RangeRequest rangeRequest,
                                                                long timestamp) {
        enterReadMethod();
        return backingKvs.getRangeWithHistory(tableName, rangeRequest, timestamp);
    }

    @Override
    @Idempotent
    public ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(String tableName,
                                                                RangeRequest rangeRequest,
                                                                long timestamp)
            throws InsufficientConsistencyException {
        enterReadMethod();
        return backingKvs.getRangeOfTimestamps(tableName, rangeRequest, timestamp);
    }

    @Override
    @Idempotent
    public Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(String tableName,
                                                                                                    Iterable<RangeRequest> rangeRequests,
                                                                                                    long timestamp) {
        enterReadMethod();
        return backingKvs.getFirstBatchForRanges(tableName, rangeRequests, timestamp);
    }

    @Override
    @Idempotent
    public void dropTable(String tableName) throws InsufficientConsistencyException {
        enterMetaMethod();
        backingKvs.dropTable(tableName);
    }

    @Override
    @Idempotent
    public void createTable(String tableName, int maxValueSizeInBytes)
            throws InsufficientConsistencyException {
        enterMetaMethod();
        backingKvs.createTable(tableName, maxValueSizeInBytes);
    }

    @Override
    @Idempotent
    public void createTables(Map<String, Integer> tableNamesToMaxValueSizeInBytes)
            throws InsufficientConsistencyException {
        enterMetaMethod();
        backingKvs.createTables(tableNamesToMaxValueSizeInBytes);
    }

    @Override
    @Idempotent
    public Set<String> getAllTableNames() {
        enterMetaMethod();
        return backingKvs.getAllTableNames();
    }

    @Override
    @Idempotent
    public byte[] getMetadataForTable(String tableName) {
        enterMetaMethod();
        return backingKvs.getMetadataForTable(tableName);
    }

    @Override
    @Idempotent
    public Map<String, byte[]> getMetadataForTables() {
        enterMetaMethod();
        return backingKvs.getMetadataForTables();
    }

    @Override
    @Idempotent
    public void putMetadataForTable(String tableName, byte[] metadata) {
        enterMetaMethod();
        backingKvs.putMetadataForTable(tableName, metadata);
    }

    @Override
    @Idempotent
    public void putMetadataForTables(Map<String, byte[]> tableNameToMetadata) {
        enterMetaMethod();
        backingKvs.putMetadataForTables(tableNameToMetadata);
    }

    @Override
    @Idempotent
    public void addGarbageCollectionSentinelValues(String tableName, Set<Cell> cells) {
        enterMetaMethod();
        backingKvs.addGarbageCollectionSentinelValues(tableName, cells);
    }

    @Override
    @Idempotent
    public Multimap<Cell, Long> getAllTimestamps(String tableName, Set<Cell> cells, long timestamp)
            throws InsufficientConsistencyException {
        enterReadMethod();
        return backingKvs.getAllTimestamps(tableName, cells, timestamp);
    }

    @Override
    public void compactInternally(String tableName) {
        enterMetaMethod();
        backingKvs.compactInternally(tableName);
    }

}
