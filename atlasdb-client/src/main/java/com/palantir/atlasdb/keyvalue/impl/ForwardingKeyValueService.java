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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ForwardingObject;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweepingRequest;
import com.palantir.atlasdb.keyvalue.api.Cell;
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
    public void close() {
        delegate().close();
    }

    @Override
    public void createTable(TableReference tableRef, final byte[] tableMetadata) {
        delegate().createTable(tableRef, tableMetadata);
    }

    @Override
    public void createTables(Map<TableReference, byte[]> tableRefToTableMetadata) {
        delegate().createTables(tableRefToTableMetadata);
    }

    @Override
    public void delete(TableReference tableRef, Multimap<Cell, Long> keys) {
        delegate().delete(tableRef, keys);
    }

    @Override
    public void deleteRange(TableReference tableRef, RangeRequest range) {
        delegate().deleteRange(tableRef, range);
    }

    @Override
    public void deleteAllTimestamps(TableReference tableRef, Map<Cell, Long> maxTimestampExclusiveByCell,
            boolean deleteSentinels) {
        delegate().deleteAllTimestamps(tableRef, maxTimestampExclusiveByCell, deleteSentinels);
    }

    @Override
    public Multimap<Cell, Long> getAllTimestamps(TableReference tableRef, Set<Cell> keys, long timestamp) {
        return delegate().getAllTimestamps(tableRef, keys, timestamp);
    }

    @Override
    public void dropTable(TableReference tableRef) {
        delegate().dropTable(tableRef);
    }

    @Override
    public void dropTables(Set<TableReference> tableRefs) {
        delegate().dropTables(tableRefs);
    }

    @Override
    public Map<Cell, Value> get(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        return delegate().get(tableRef, timestampByCell);
    }

    @Override
    public Map<Cell, Value> getRows(TableReference tableRef, Iterable<byte[]> rows,
                                    ColumnSelection columnSelection, long timestamp) {
        return delegate().getRows(tableRef, rows, columnSelection, timestamp);
    }

    @Override
    public Map<byte[], RowColumnRangeIterator> getRowsColumnRange(TableReference tableRef, Iterable<byte[]> rows,
            BatchColumnRangeSelection batchColumnRangeSelection, long timestamp) {
        return delegate().getRowsColumnRange(tableRef, rows, batchColumnRangeSelection, timestamp);
    }

    @Override
    public RowColumnRangeIterator getRowsColumnRange(TableReference tableRef,
                                                     Iterable<byte[]> rows,
                                                     ColumnRangeSelection columnRangeSelection,
                                                     int cellBatchHint,
                                                     long timestamp) {
        return delegate().getRowsColumnRange(tableRef, rows, columnRangeSelection, cellBatchHint, timestamp);
    }

    @Override
    public Map<Cell, Long> getLatestTimestamps(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        return delegate().getLatestTimestamps(tableRef, timestampByCell);
    }

    @Override
    public ClosableIterator<RowResult<Value>> getRange(TableReference tableRef, RangeRequest rangeRequest,
            long timestamp) {
        return delegate().getRange(tableRef, rangeRequest, timestamp);
    }

    @Override
    public ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(TableReference tableRef,
                                                                       RangeRequest rangeRequest,
                                                                       long timestamp) {
        return delegate().getRangeOfTimestamps(tableRef, rangeRequest, timestamp);
    }

    public ClosableIterator<List<CandidateCellForSweeping>> getCandidateCellsForSweeping(
                    TableReference tableRef,
                    CandidateCellForSweepingRequest request) {
        return delegate().getCandidateCellsForSweeping(tableRef, request);
    }

    @Override
    public Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(
            TableReference tableRef, Iterable<RangeRequest> rangeRequests, long timestamp) {
        return delegate().getFirstBatchForRanges(tableRef, rangeRequests, timestamp);
    }

    @Override
    public boolean isInitialized() {
        return delegate().isInitialized();
    }

    @Override
    public void put(TableReference tableRef, Map<Cell, byte[]> values, long timestamp) {
        delegate().put(tableRef, values, timestamp);
    }

    @Override
    public void multiPut(Map<TableReference, ? extends Map<Cell, byte[]>> valuesByTable, long timestamp) {
        delegate().multiPut(valuesByTable, timestamp);
    }

    @Override
    public void putWithTimestamps(TableReference tableRef, Multimap<Cell, Value> values) {
        delegate().putWithTimestamps(tableRef, values);
    }

    @Override
    public void putUnlessExists(TableReference tableRef, Map<Cell, byte[]> values) throws KeyAlreadyExistsException {
        delegate().putUnlessExists(tableRef, values);
    }

    @Override
    public boolean supportsCheckAndSet() {
        return delegate().supportsCheckAndSet();
    }

    @Override
    public void checkAndSet(CheckAndSetRequest request) {
        delegate().checkAndSet(request);
    }

    @Override
    public void truncateTable(TableReference tableRef) {
        delegate().truncateTable(tableRef);
    }

    @Override
    public void truncateTables(Set<TableReference> tableRefs) {
        delegate().truncateTables(tableRefs);
    }

    @Override
    public byte[] getMetadataForTable(TableReference tableRef) {
        return delegate().getMetadataForTable(tableRef);
    }

    @Override
    public Map<TableReference, byte[]> getMetadataForTables() {
        return delegate().getMetadataForTables();
    }

    @Override
    public void putMetadataForTable(TableReference tableRef, byte[] metadata) {
        delegate().putMetadataForTable(tableRef, metadata);
    }
    @Override
    public void putMetadataForTables(final Map<TableReference, byte[]> tableRefToMetadata) {
        delegate().putMetadataForTables(tableRefToMetadata);
    }

    @Override
    public Set<TableReference> getAllTableNames() {
        return delegate().getAllTableNames();
    }

    @Override
    public void addGarbageCollectionSentinelValues(TableReference tableRef, Iterable<Cell> cells) {
        delegate().addGarbageCollectionSentinelValues(tableRef, cells);
    }

    @Override
    public void compactInternally(TableReference tableRef) {
        delegate().compactInternally(tableRef);
    }

    @Override
    public ClusterAvailabilityStatus getClusterAvailabilityStatus() {
        return delegate().getClusterAvailabilityStatus();
    }

    @Override
    public void compactInternally(TableReference tableRef, boolean inMaintenanceMode) {
        delegate().compactInternally(tableRef, inMaintenanceMode);
    }

    @Override
    public boolean shouldTriggerCompactions() {
        return delegate().shouldTriggerCompactions();
    }
}
