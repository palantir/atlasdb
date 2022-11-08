/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.impl.expectations;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweepingRequest;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.ForwardingKeyValueService;
import com.palantir.atlasdb.transaction.api.expectations.TransactionReadInfo;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.exception.AtlasDbDependencyException;
import com.palantir.util.paging.TokenBackedBasicResultsPage;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

public class TrackingKeyValueServiceImpl extends ForwardingKeyValueService implements TrackingKeyValueService {
    KeyValueService delegate;
    KeyValueServiceDataTracker tracker = new KeyValueServiceDataTracker();

    public TrackingKeyValueServiceImpl(KeyValueService delegate) {
        this.delegate = delegate;
    }

    @Override
    public KeyValueService delegate() {
        return delegate;
    }

    @Override
    public TransactionReadInfo getOverallReadInfo() {
        return tracker.getReadInfo();
    }

    @Override
    public ImmutableMap<TableReference, TransactionReadInfo> getReadInfoByTable() {
        return tracker.getReadInfoByTable();
    }

    @Override
    public ListenableFuture<Map<Cell, Value>> getAsync(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        return Futures.transform(
                delegate.getAsync(tableRef, timestampByCell),
                valueByCell -> {
                    tracker.readForTable(tableRef, "getAsync", ExpectationsMeasuringUtils.sizeInBytes(valueByCell));
                    return valueByCell;
                },
                MoreExecutors.directExecutor());
    }

    @Override
    public Map<Cell, Value> getRows(
            TableReference tableRef, Iterable<byte[]> rows, ColumnSelection columnSelection, long timestamp) {
        Map<Cell, Value> result = delegate.getRows(tableRef, rows, columnSelection, timestamp);
        tracker.readForTable(tableRef, "getRows", ExpectationsMeasuringUtils.sizeInBytes(result));
        return result;
    }

    @Override
    public Map<byte[], RowColumnRangeIterator> getRowsColumnRange(
            TableReference tableRef,
            Iterable<byte[]> rows,
            BatchColumnRangeSelection batchColumnRangeSelection,
            long timestamp) {
        tracker.callForTable(tableRef);
        Map<byte[], RowColumnRangeIterator> result =
                delegate.getRowsColumnRange(tableRef, rows, batchColumnRangeSelection, timestamp);
        // todo(aalouane) this throws if delegate returns an immutable map or a map that does not implement replaceAll
        // not sure what to do here
        result.replaceAll((rowsRead, iterator) -> {
            tracker.partialReadForTable(tableRef, rowsRead.length);
            return new TrackingRowColumnRangeIterator(
                    iterator, partialReadForTableConsumer(tableRef), ExpectationsMeasuringUtils::sizeInBytes);
        });
        return result;
    }

    @Override
    public RowColumnRangeIterator getRowsColumnRange(
            TableReference tableRef,
            Iterable<byte[]> rows,
            ColumnRangeSelection columnRangeSelection,
            int cellBatchHint,
            long timestamp) {
        tracker.callForTable(tableRef);
        RowColumnRangeIterator result =
                delegate.getRowsColumnRange(tableRef, rows, columnRangeSelection, cellBatchHint, timestamp);

        return new TrackingRowColumnRangeIterator(
                result, partialReadForTableConsumer(tableRef), ExpectationsMeasuringUtils::sizeInBytes);
    }

    @Override
    public Map<Cell, Value> get(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        Map<Cell, Value> result = delegate.get(tableRef, timestampByCell);
        tracker.readForTable(tableRef, "get", ExpectationsMeasuringUtils.sizeInBytes(result));
        return result;
    }

    @Override
    public Map<Cell, Long> getLatestTimestamps(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        Map<Cell, Long> result = delegate.getLatestTimestamps(tableRef, timestampByCell);
        tracker.readForTable(tableRef, "getLatestTimestamps", ExpectationsMeasuringUtils.toLongSizeInBytes(result));
        return result;
    }

    @Override
    public ClosableIterator<RowResult<Value>> getRange(
            TableReference tableRef, RangeRequest rangeRequest, long timestamp) {
        tracker.callForTable(tableRef);
        try (ClosableIterator<RowResult<Value>> result = delegate.getRange(tableRef, rangeRequest, timestamp)) {
            return new TrackingClosableIterator<>(
                    result, partialReadForTableConsumer(tableRef), ExpectationsMeasuringUtils::sizeInBytes);
        }
    }

    @Override
    public ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(
            TableReference tableRef, RangeRequest rangeRequest, long timestamp)
            throws InsufficientConsistencyException {
        tracker.callForTable(tableRef);
        try (ClosableIterator<RowResult<Set<Long>>> result =
                delegate.getRangeOfTimestamps(tableRef, rangeRequest, timestamp)) {
            return new TrackingClosableIterator<>(
                    result, partialReadForTableConsumer(tableRef), ExpectationsMeasuringUtils::setResultSizeInBytes);
        }
    }

    @Override
    public ClosableIterator<List<CandidateCellForSweeping>> getCandidateCellsForSweeping(
            TableReference tableRef, CandidateCellForSweepingRequest request) {
        tracker.callForTable(tableRef);
        try (ClosableIterator<List<CandidateCellForSweeping>> result =
                delegate.getCandidateCellsForSweeping(tableRef, request)) {
            return new TrackingClosableIterator<>(
                    result, partialReadForTableConsumer(tableRef), ExpectationsMeasuringUtils::sizeInBytes);
        }
    }

    @Override
    public Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(
            TableReference tableRef, Iterable<RangeRequest> rangeRequests, long timestamp) {
        Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> result =
                delegate.getFirstBatchForRanges(tableRef, rangeRequests, timestamp);
        tracker.readForTable(
                tableRef, "getFirstBatchForRanges", ExpectationsMeasuringUtils.pageByRequestSizeInBytes(result));
        return result;
    }

    @Override
    public Set<TableReference> getAllTableNames() {
        Set<TableReference> result = delegate.getAllTableNames();
        tracker.tableAgnosticRead("getAllTableNames", ExpectationsMeasuringUtils.sizeInBytes(result));
        return result;
    }

    @Override
    public byte[] getMetadataForTable(TableReference tableRef) {
        byte[] result = delegate.getMetadataForTable(tableRef);
        tracker.tableAgnosticRead("getMetadataForTable", result.length);
        return result;
    }

    @Override
    public Map<TableReference, byte[]> getMetadataForTables() {
        Map<TableReference, byte[]> result = delegate.getMetadataForTables();
        tracker.tableAgnosticRead("getMetadataForTables", ExpectationsMeasuringUtils.toArraySizeInBytes(result));
        return result;
    }

    @Override
    public Multimap<Cell, Long> getAllTimestamps(TableReference tableRef, Set<Cell> cells, long timestamp)
            throws AtlasDbDependencyException {
        Multimap<Cell, Long> result = delegate.getAllTimestamps(tableRef, cells, timestamp);
        tracker.readForTable(tableRef, "getAllTimestamps", ExpectationsMeasuringUtils.sizeInBytes(result));
        return result;
    }

    @Override
    public List<byte[]> getRowKeysInRange(TableReference tableRef, byte[] startRow, byte[] endRow, int maxResults) {
        List<byte[]> result = delegate.getRowKeysInRange(tableRef, startRow, endRow, maxResults);
        tracker.readForTable(tableRef, "getRowKeysInRange", ExpectationsMeasuringUtils.byteArraysSizeInBytes(result));
        return result;
    }

    private Consumer<Long> partialReadForTableConsumer(TableReference tableRef) {
        return bytes -> tracker.partialReadForTable(tableRef, bytes);
    }
}
