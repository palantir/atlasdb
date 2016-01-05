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

import com.google.common.collect.ForwardingObject;
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

public abstract class ForwardingKeyValueService extends ForwardingObject implements KeyValueService {
    @Override
    protected abstract KeyValueService delegate();

    @Override
    public Collection<? extends KeyValueService> getDelegates() {
        return ImmutableList.of(delegate());
    }

    @Override
    public void initializeFromFreshInstance() {
        delegate().initializeFromFreshInstance();
    }

    @Override
    public void close() {
        delegate().close();
    }

    @Override
    public void teardown() {
        delegate().teardown();
    }

    @Override
    public void createTable(String tableName, final byte[] tableMetadata) {
        delegate().createTable(tableName, tableMetadata);
    }

    @Override
    public void createTables(Map<String, byte[]> tableNameToTableMetadata) {
        delegate().createTables(tableNameToTableMetadata);
    }

    @Override
    public void delete(String tableName, Multimap<Cell, Long> keys) {
        delegate().delete(tableName, keys);
    }

    @Override
    public Multimap<Cell, Long> getAllTimestamps(String tableName, Set<Cell> keys, long timestamp) {
        return delegate().getAllTimestamps(tableName, keys, timestamp);
    }

    @Override
    public void dropTable(String tableName) {
        delegate().dropTable(tableName);
    }

    @Override
    public void dropTables(Set<String> tableNames) {
        delegate().dropTables(tableNames);
    }

    @Override
    public Map<Cell, Value> get(String tableName, Map<Cell, Long> timestampByCell) {
        return delegate().get(tableName, timestampByCell);
    }

    @Override
    public Map<Cell, Value> getRows(String tableName, Iterable<byte[]> rows,
                                    ColumnSelection columnSelection, long timestamp) {
        return delegate().getRows(tableName, rows, columnSelection, timestamp);
    }

    @Override
    public Map<Cell, Long> getLatestTimestamps(String tableName, Map<Cell, Long> timestampByCell) {
        return delegate().getLatestTimestamps(tableName, timestampByCell);
    }

    @Override
    public ClosableIterator<RowResult<Value>> getRange(String tableName, RangeRequest rangeRequest, long timestamp) {
        return delegate().getRange(tableName, rangeRequest, timestamp);
    }

    @Override
    public ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(String tableName,
                                                                       RangeRequest rangeRequest,
                                                                       long timestamp) {
        return delegate().getRangeOfTimestamps(tableName, rangeRequest, timestamp);
    }

    @Override
    public ClosableIterator<RowResult<Set<Value>>> getRangeWithHistory(String tableName,
                                                                       RangeRequest rangeRequest,
                                                                       long timestamp) {
        return delegate().getRangeWithHistory(tableName, rangeRequest, timestamp);
    }

    @Override
    public Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(String tableName,
            Iterable<RangeRequest> rangeRequests,
            long timestamp) {
        return delegate().getFirstBatchForRanges(tableName, rangeRequests, timestamp);
    }

    @Override
    public void put(String tableName, Map<Cell, byte[]> values, long timestamp) {
        delegate().put(tableName, values, timestamp);
    }

    @Override
    public void multiPut(Map<String, ? extends Map<Cell, byte[]>> valuesByTable, long timestamp) {
        delegate().multiPut(valuesByTable, timestamp);
    }

    @Override
    public void putWithTimestamps(String tableName, Multimap<Cell, Value> values) {
        delegate().putWithTimestamps(tableName, values);
    }

    @Override
    public void putUnlessExists(String tableName, Map<Cell, byte[]> values) throws KeyAlreadyExistsException {
        delegate().putUnlessExists(tableName, values);
    }

    @Override
    public void truncateTable(String tableName) {
        delegate().truncateTable(tableName);
    }

    @Override
    public void truncateTables(Set<String> tableNames) {
        delegate().truncateTables(tableNames);
    }

    @Override
    public byte[] getMetadataForTable(String tableName) {
        return delegate().getMetadataForTable(tableName);
    }

    @Override
    public Map<String, byte[]> getMetadataForTables() {
        return delegate().getMetadataForTables();
    }

    @Override
    public void putMetadataForTable(String tableName, byte[] metadata) {
        delegate().putMetadataForTable(tableName, metadata);
    }
    @Override
    public void putMetadataForTables(final Map<String, byte[]> tableNameToMetadata) {
        delegate().putMetadataForTables(tableNameToMetadata);
    }

    @Override
    public Set<String> getAllTableNames() {
        return delegate().getAllTableNames();
    }

    @Override
    public void addGarbageCollectionSentinelValues(String tableName, Set<Cell> cells) {
        delegate().addGarbageCollectionSentinelValues(tableName, cells);
    }

    @Override
    public void compactInternally(String tableName) {
        delegate().compactInternally(tableName);
    }
}
