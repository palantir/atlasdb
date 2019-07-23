/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.transaction.impl.illiteracy;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweepingRequest;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetCompatibility;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.ClusterAvailabilityStatus;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.TimestampRangeDelete;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.exception.AtlasDbDependencyException;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

// For the most part this could be an AutoDelegate, but that road is fraught with peril.
public class WatchAwareKeyValueService implements KeyValueService {
    private final KeyValueService delegate;
    private final WatchRegistry watchRegistry;

    public WatchAwareKeyValueService(
            KeyValueService delegate,
            WatchRegistry watchRegistry) {
        this.delegate = delegate;
        this.watchRegistry = watchRegistry;
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public Collection<? extends KeyValueService> getDelegates() {
        return ImmutableList.of(delegate);
    }

    @Override
    public Map<Cell, Value> getRows(TableReference tableRef, Iterable<byte[]> rows, ColumnSelection columnSelection,
            long timestamp) {
        return delegate.getRows(tableRef, rows, columnSelection, timestamp);
    }

    @Override
    public Map<byte[], RowColumnRangeIterator> getRowsColumnRange(TableReference tableRef, Iterable<byte[]> rows,
            BatchColumnRangeSelection batchColumnRangeSelection, long timestamp) {
        return delegate.getRowsColumnRange(tableRef, rows, batchColumnRangeSelection, timestamp);
    }

    @Override
    public RowColumnRangeIterator getRowsColumnRange(TableReference tableRef, Iterable<byte[]> rows,
            ColumnRangeSelection columnRangeSelection, int cellBatchHint, long timestamp) {
        return delegate.getRowsColumnRange(tableRef, rows, columnRangeSelection, cellBatchHint, timestamp);
    }

    @Override
    public Map<Cell, Value> get(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        return delegate.get(tableRef, timestampByCell);
    }

    @Override
    public Map<Cell, Long> getLatestTimestamps(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        return delegate.getLatestTimestamps(tableRef, timestampByCell);
    }

    @Override
    public void put(TableReference tableRef, Map<Cell, byte[]> values, long timestamp)
            throws KeyAlreadyExistsException {
        delegate.put(tableRef, values, timestamp);
    }

    @Override
    public void multiPut(Map<TableReference, ? extends Map<Cell, byte[]>> valuesByTable, long timestamp)
            throws KeyAlreadyExistsException {
        delegate.multiPut(valuesByTable, timestamp);
    }

    @Override
    public void putWithTimestamps(TableReference tableRef, Multimap<Cell, Value> cellValues)
            throws KeyAlreadyExistsException {
        delegate.putWithTimestamps(tableRef, cellValues);
    }

    @Override
    public void putUnlessExists(TableReference tableRef, Map<Cell, byte[]> values) throws KeyAlreadyExistsException {
        delegate.putUnlessExists(tableRef, values);
    }

    @Override
    public CheckAndSetCompatibility getCheckAndSetCompatibility() {
        return delegate.getCheckAndSetCompatibility();
    }

    @Override
    public void checkAndSet(CheckAndSetRequest checkAndSetRequest) throws CheckAndSetException {
        delegate.checkAndSet(checkAndSetRequest);
    }

    @Override
    public void delete(TableReference tableRef, Multimap<Cell, Long> keys) {
        delegate.delete(tableRef, keys);
    }

    @Override
    public void deleteRange(TableReference tableRef, RangeRequest range) {
        delegate.deleteRange(tableRef, range);
    }

    @Override
    public void deleteAllTimestamps(TableReference tableRef, Map<Cell, TimestampRangeDelete> deletes)
            throws InsufficientConsistencyException {
        delegate.deleteAllTimestamps(tableRef, deletes);
    }

    @Override
    public void truncateTable(TableReference tableRef) throws InsufficientConsistencyException {
        delegate.truncateTable(tableRef);
    }

    @Override
    public void truncateTables(Set<TableReference> tableRefs) throws InsufficientConsistencyException {
        delegate.truncateTables(tableRefs);
    }

    @Override
    public ClosableIterator<RowResult<Value>> getRange(TableReference tableRef, RangeRequest rangeRequest,
            long timestamp) {
        return delegate.getRange(tableRef, rangeRequest, timestamp);
    }

    @Override
    public ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(TableReference tableRef,
            RangeRequest rangeRequest, long timestamp) throws InsufficientConsistencyException {
        return delegate.getRangeOfTimestamps(tableRef, rangeRequest, timestamp);
    }

    @Override
    public ClosableIterator<List<CandidateCellForSweeping>> getCandidateCellsForSweeping(TableReference tableRef,
            CandidateCellForSweepingRequest request) {
        return delegate.getCandidateCellsForSweeping(tableRef, request);
    }

    @Override
    public Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(
            TableReference tableRef, Iterable<RangeRequest> rangeRequests, long timestamp) {
        return delegate.getFirstBatchForRanges(tableRef, rangeRequests, timestamp);
    }

    @Override
    public void dropTable(TableReference tableRef) throws InsufficientConsistencyException {
        delegate.dropTable(tableRef);
    }

    @Override
    public void dropTables(Set<TableReference> tableRefs) throws InsufficientConsistencyException {
        delegate.dropTables(tableRefs);
    }

    @Override
    public void createTable(TableReference tableRef, byte[] tableMetadata) throws InsufficientConsistencyException {
        delegate.createTable(tableRef, tableMetadata);
    }

    @Override
    public void createTables(Map<TableReference, byte[]> tableRefToTableMetadata)
            throws InsufficientConsistencyException {
        delegate.createTables(tableRefToTableMetadata);
    }

    @Override
    public Set<TableReference> getAllTableNames() {
        return delegate.getAllTableNames();
    }

    @Override
    public byte[] getMetadataForTable(TableReference tableRef) {
        return delegate.getMetadataForTable(tableRef);
    }

    @Override
    public Map<TableReference, byte[]> getMetadataForTables() {
        return delegate.getMetadataForTables();
    }

    @Override
    public void putMetadataForTable(TableReference tableRef, byte[] metadata) {
        delegate.putMetadataForTable(tableRef, metadata);
    }

    @Override
    public void putMetadataForTables(Map<TableReference, byte[]> tableRefToMetadata) {
        delegate.putMetadataForTables(tableRefToMetadata);
    }

    @Override
    public void addGarbageCollectionSentinelValues(TableReference tableRef, Iterable<Cell> cells) {
        delegate.addGarbageCollectionSentinelValues(tableRef, cells);
    }

    @Override
    public Multimap<Cell, Long> getAllTimestamps(TableReference tableRef, Set<Cell> cells, long timestamp)
            throws AtlasDbDependencyException {
        return delegate.getAllTimestamps(tableRef, cells, timestamp);
    }

    @Override
    public void compactInternally(TableReference tableRef) {
        delegate.compactInternally(tableRef);
    }

    @Override
    public ClusterAvailabilityStatus getClusterAvailabilityStatus() {
        return delegate.getClusterAvailabilityStatus();
    }
}
