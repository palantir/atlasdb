/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.keyvalue.impl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweepingRequest;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetCompatibility;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.ClusterAvailabilityStatus;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.TimestampRangeDelete;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.common.base.ClosableIterator;
import com.palantir.util.paging.TokenBackedBasicResultsPage;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    public void close() {
        delegate1.close();
        delegate2.close();
    }

    @Override
    public Collection<? extends KeyValueService> getDelegates() {
        return ImmutableList.of(delegate1, delegate2);
    }

    @Override
    public Map<Cell, Value> getRows(
            TableReference tableRef, Iterable<byte[]> rows, ColumnSelection columnSelection, long timestamp) {
        return delegate1.getRows(tableRef, rows, columnSelection, timestamp);
    }

    @Override
    public Map<Cell, Value> get(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        return delegate1.get(tableRef, timestampByCell);
    }

    @Override
    public Map<Cell, Long> getLatestTimestamps(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        return delegate1.getLatestTimestamps(tableRef, timestampByCell);
    }

    @Override
    public void put(TableReference tableRef, Map<Cell, byte[]> values, long timestamp) {
        delegate1.put(tableRef, values, timestamp);
        delegate2.put(tableRef, values, timestamp);
    }

    @Override
    public void multiPut(Map<TableReference, ? extends Map<Cell, byte[]>> valuesByTable, long timestamp) {
        delegate1.multiPut(valuesByTable, timestamp);
        delegate2.multiPut(valuesByTable, timestamp);
    }

    @Override
    public void putWithTimestamps(TableReference tableRef, Multimap<Cell, Value> values) {
        delegate1.putWithTimestamps(tableRef, values);
        delegate2.putWithTimestamps(tableRef, values);
    }

    @Override
    public void putUnlessExists(TableReference tableRef, Map<Cell, byte[]> values) throws KeyAlreadyExistsException {
        delegate1.putUnlessExists(tableRef, values);
    }

    @Override
    public void setOnce(TableReference tableRef, Map<Cell, byte[]> values) {
        delegate1.setOnce(tableRef, values);
    }

    @Override
    public CheckAndSetCompatibility getCheckAndSetCompatibility() {
        return delegate1.getCheckAndSetCompatibility();
    }

    @Override
    public void checkAndSet(CheckAndSetRequest checkAndSetRequest) {
        delegate1.checkAndSet(checkAndSetRequest);
    }

    @Override
    public void delete(TableReference tableRef, Multimap<Cell, Long> keys) {
        delegate1.delete(tableRef, keys);
        delegate2.delete(tableRef, keys);
    }

    @Override
    public void deleteRange(TableReference tableRef, RangeRequest range) {
        delegate1.deleteRange(tableRef, range);
        delegate2.deleteRange(tableRef, range);
    }

    @Override
    public void deleteRows(TableReference tableRef, Iterable<byte[]> rows) {
        delegate1.deleteRows(tableRef, rows);
        delegate2.deleteRows(tableRef, rows);
    }

    @Override
    public void deleteAllTimestamps(TableReference tableRef, Map<Cell, TimestampRangeDelete> deletes) {
        delegate1.deleteAllTimestamps(tableRef, deletes);
        delegate2.deleteAllTimestamps(tableRef, deletes);
    }

    @Override
    public void truncateTable(TableReference tableRef) {
        delegate1.truncateTable(tableRef);
        delegate2.truncateTable(tableRef);
    }

    @Override
    public void truncateTables(Set<TableReference> tableRefs) {
        delegate1.truncateTables(tableRefs);
        delegate2.truncateTables(tableRefs);
    }

    @Override
    public ClosableIterator<RowResult<Value>> getRange(
            TableReference tableRef, RangeRequest rangeRequest, long timestamp) {
        return delegate1.getRange(tableRef, rangeRequest, timestamp);
    }

    @Override
    public Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(
            TableReference tableRef, Iterable<RangeRequest> rangeRequests, long timestamp) {
        return delegate1.getFirstBatchForRanges(tableRef, rangeRequests, timestamp);
    }

    @Override
    public void dropTable(TableReference tableRef) {
        delegate1.dropTable(tableRef);
        delegate2.dropTable(tableRef);
    }

    @Override
    public void dropTables(Set<TableReference> tableRefs) {
        for (TableReference tableRef : tableRefs) {
            delegate1.dropTable(tableRef);
            delegate2.dropTable(tableRef);
        }
    }

    @Override
    public void createTable(TableReference tableRef, byte[] tableMetadata) {
        delegate1.createTable(tableRef, tableMetadata);
        delegate2.createTable(tableRef, tableMetadata);
    }

    @Override
    public Set<TableReference> getAllTableNames() {
        return delegate1.getAllTableNames();
    }

    @Override
    public byte[] getMetadataForTable(TableReference tableRef) {
        return delegate1.getMetadataForTable(tableRef);
    }

    @Override
    public Map<TableReference, byte[]> getMetadataForTables() {
        return delegate1.getMetadataForTables();
    }

    @Override
    public void putMetadataForTable(TableReference tableRef, byte[] metadata) {
        delegate1.putMetadataForTable(tableRef, metadata);
        delegate2.putMetadataForTable(tableRef, metadata);
    }

    @Override
    public void addGarbageCollectionSentinelValues(TableReference tableRef, Iterable<Cell> cells) {
        delegate1.addGarbageCollectionSentinelValues(tableRef, cells);
        delegate2.addGarbageCollectionSentinelValues(tableRef, cells);
    }

    @Override
    public Multimap<Cell, Long> getAllTimestamps(TableReference tableRef, Set<Cell> cells, long timestamp) {
        return delegate1.getAllTimestamps(tableRef, cells, timestamp);
    }

    @Override
    public ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(
            TableReference tableRef, RangeRequest rangeRequest, long timestamp) {
        return delegate1.getRangeOfTimestamps(tableRef, rangeRequest, timestamp);
    }

    @Override
    public ClosableIterator<List<CandidateCellForSweeping>> getCandidateCellsForSweeping(
            TableReference tableRef, CandidateCellForSweepingRequest request) {
        return delegate1.getCandidateCellsForSweeping(tableRef, request);
    }

    @Override
    public void createTables(Map<TableReference, byte[]> tableRefToTableMetadata) {
        delegate1.createTables(tableRefToTableMetadata);
        delegate2.createTables(tableRefToTableMetadata);
    }

    @Override
    public void putMetadataForTables(Map<TableReference, byte[]> tableRefToMetadata) {
        delegate1.putMetadataForTables(tableRefToMetadata);
        delegate2.putMetadataForTables(tableRefToMetadata);
    }

    @Override
    public void compactInternally(TableReference tableRef) {
        delegate1.compactInternally(tableRef);
        delegate2.compactInternally(tableRef);
    }

    @Override
    public ClusterAvailabilityStatus getClusterAvailabilityStatus() {
        ClusterAvailabilityStatus clusterAvailabilityStatus1 = delegate1.getClusterAvailabilityStatus();
        ClusterAvailabilityStatus clusterAvailabilityStatus2 = delegate2.getClusterAvailabilityStatus();

        if (clusterAvailabilityStatus1.compareTo(clusterAvailabilityStatus2) < 0) {
            return clusterAvailabilityStatus1;
        }
        return clusterAvailabilityStatus2;
    }

    @Override
    public boolean isInitialized() {
        return delegate1.isInitialized() && delegate2.isInitialized();
    }

    @Override
    public Map<byte[], RowColumnRangeIterator> getRowsColumnRange(
            TableReference tableRef,
            Iterable<byte[]> rows,
            BatchColumnRangeSelection batchColumnRangeSelection,
            long timestamp) {
        return delegate1.getRowsColumnRange(tableRef, rows, batchColumnRangeSelection, timestamp);
    }

    @Override
    public RowColumnRangeIterator getRowsColumnRange(
            TableReference tableRef,
            Iterable<byte[]> rows,
            ColumnRangeSelection columnRangeSelection,
            int cellBatchHint,
            long timestamp) {
        return delegate1.getRowsColumnRange(tableRef, rows, columnRangeSelection, cellBatchHint, timestamp);
    }

    @Override
    public void compactInternally(TableReference tableRef, boolean inMaintenanceMode) {
        delegate1.compactInternally(tableRef, inMaintenanceMode);
        delegate2.compactInternally(tableRef, inMaintenanceMode);
    }

    @Override
    public boolean shouldTriggerCompactions() {
        return delegate1.shouldTriggerCompactions() || delegate2.shouldTriggerCompactions();
    }

    @Override
    public List<byte[]> getRowKeysInRange(TableReference tableRef, byte[] startRow, byte[] endRow, int maxResults) {
        return delegate1.getRowKeysInRange(tableRef, startRow, endRow, maxResults);
    }

    @Override
    public ListenableFuture<Map<Cell, Value>> getAsync(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        return delegate1.getAsync(tableRef, timestampByCell);
    }
}
