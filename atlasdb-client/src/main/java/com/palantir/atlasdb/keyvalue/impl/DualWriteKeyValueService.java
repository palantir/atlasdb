/**
 * Copyright 2015 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.impl;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.common.base.ClosableIterator;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

/**
 * An implementation of KeyValueService which delegates reads to the first KeyValueService and
 * writes to both, except for putUnlessExists, which only goes to the first KeyValueService.
 *
 * This is useful for Migration.
 */
public class DualWriteKeyValueService implements KeyValueService {
    private final KeyValueService delegate1;
    private final KeyValueService delegate2;

    public DualWriteKeyValueService(KeyValueService delegate1, KeyValueService delegate2) {
        this.delegate1 = delegate1;
        this.delegate2 = delegate2;
    }

    @Override
    public void initializeFromFreshInstance() {
        delegate1.initializeFromFreshInstance();
        delegate2.initializeFromFreshInstance();
    }

    @Override
    public void close() {
        delegate1.close();
        delegate2.close();
    }

    @Override
    public void teardown() {
        delegate1.teardown();
        delegate2.teardown();
    }

    @Override
    public Collection<? extends KeyValueService> getDelegates() {
        return ImmutableList.of(delegate1, delegate2);
    }

    @Override
    public Map<Cell, Value> getRows(String tableName,
                                    Iterable<byte[]> rows,
                                    ColumnSelection columnSelection,
                                    long timestamp) {
        return delegate1.getRows(tableName, rows, columnSelection, timestamp);
    }

    @Override
    public Map<Cell, Value> get(String tableName, Map<Cell, Long> timestampByCell) {
        return delegate1.get(tableName, timestampByCell);
    }

    @Override
    public Map<Cell, Long> getLatestTimestamps(String tableName, Map<Cell, Long> timestampByCell) {
        return delegate1.getLatestTimestamps(tableName, timestampByCell);
    }

    @Override
    public void put(String tableName, Map<Cell, byte[]> values, long timestamp) {
        delegate1.put(tableName, values, timestamp);
        delegate2.put(tableName, values, timestamp);
    }

    @Override
    public void multiPut(Map<String, ? extends Map<Cell, byte[]>> valuesByTable, long timestamp) {
        delegate1.multiPut(valuesByTable, timestamp);
        delegate2.multiPut(valuesByTable, timestamp);
    }

    @Override
    public void putWithTimestamps(String tableName, Multimap<Cell, Value> values) {
        delegate1.putWithTimestamps(tableName, values);
        delegate2.putWithTimestamps(tableName, values);
    }

    @Override
    public void putUnlessExists(String tableName, Map<Cell, byte[]> values)
            throws KeyAlreadyExistsException {
        delegate1.putUnlessExists(tableName, values);
    }

    @Override
    public void delete(String tableName, Multimap<Cell, Long> keys) {
        delegate1.delete(tableName, keys);
        delegate2.delete(tableName, keys);
    }

    @Override
    public void truncateTable(String tableName) {
        delegate1.truncateTable(tableName);
        delegate2.truncateTable(tableName);
    }

    @Override
    public void truncateTables(Set<String> tableNames) {
        delegate1.truncateTables(tableNames);
        delegate2.truncateTables(tableNames);
    }

    @Override
    public ClosableIterator<RowResult<Value>> getRange(String tableName, RangeRequest rangeRequest, long timestamp) {
        return delegate1.getRange(tableName, rangeRequest, timestamp);
    }

    @Override
    public Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(String tableName,
                                                                                                           Iterable<RangeRequest> rangeRequests,
                                                                                                           long timestamp) {
        return delegate1.getFirstBatchForRanges(tableName, rangeRequests, timestamp);
    }

    @Override
    public void dropTable(String tableName) {
        delegate1.dropTable(tableName);
        delegate2.dropTable(tableName);
    }

    @Override
    public void dropTables(Set<String> tableNames) {
        for (String tableName : tableNames) {
            delegate1.dropTable(tableName);
            delegate2.dropTable(tableName);
        }
    }

    @Override
    public void createTable(String tableName, byte[] tableMetadata) {
        delegate1.createTable(tableName, tableMetadata);
        delegate2.createTable(tableName, tableMetadata);
    }

    @Override
    public Set<String> getAllTableNames() {
        return delegate1.getAllTableNames();
    }

    @Override
    public byte[] getMetadataForTable(String tableName) {
        return delegate1.getMetadataForTable(tableName);
    }

    @Override
    public Map<String, byte[]> getMetadataForTables() {
        return delegate1.getMetadataForTables();
    }

    @Override
    public void putMetadataForTable(String tableName, byte[] metadata) {
        delegate1.putMetadataForTable(tableName, metadata);
        delegate2.putMetadataForTable(tableName, metadata);
    }

    @Override
    public void addGarbageCollectionSentinelValues(String tableName, Set<Cell> cells) {
        delegate1.addGarbageCollectionSentinelValues(tableName, cells);
        delegate2.addGarbageCollectionSentinelValues(tableName, cells);
    }

    @Override
    public Multimap<Cell, Long> getAllTimestamps(String tableName, Set<Cell> cells, long timestamp) {
        return delegate1.getAllTimestamps(tableName, cells, timestamp);
    }

    @Override
    public ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(String tableName, RangeRequest rangeRequest, long timestamp) {
        return delegate1.getRangeOfTimestamps(tableName, rangeRequest, timestamp);
    }

    @Override
    public ClosableIterator<RowResult<Set<Value>>> getRangeWithHistory(String tableName, RangeRequest rangeRequest, long timestamp) {
        return delegate1.getRangeWithHistory(tableName, rangeRequest, timestamp);
    }

    @Override
    public void createTables(Map<String, byte[]> tableNameToTableMetadata) {
        delegate1.createTables(tableNameToTableMetadata);
        delegate2.createTables(tableNameToTableMetadata);
    }

    @Override
    public void putMetadataForTables(Map<String, byte[]> tableNameToMetadata) {
        delegate1.putMetadataForTables(tableNameToMetadata);
        delegate2.putMetadataForTables(tableNameToMetadata);
    }

    @Override
    public void compactInternally(String tableName) {
        delegate1.compactInternally(tableName);
        delegate2.compactInternally(tableName);
    }
}
