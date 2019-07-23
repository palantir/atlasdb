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
import java.util.SortedMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.primitives.UnsignedBytes;
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
import com.palantir.atlasdb.keyvalue.impl.LocalRowColumnRangeIterator;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManager;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.exception.AtlasDbDependencyException;
import com.palantir.common.streams.KeyedStream;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

// For the most part this could be an AutoDelegate, but that road is fraught with peril.
public class RowWatchAwareKeyValueService implements KeyValueService {
    private static final Logger log = LoggerFactory.getLogger(RowWatchAwareKeyValueService.class);

    private final KeyValueService delegate;
    private final ConflictDetectionManager conflictDetectionManager;
    private final RowCacheReader rowCacheReader;
    private final AtomicLong readCount;

    public RowWatchAwareKeyValueService(
            KeyValueService delegate,
            WatchRegistry watchRegistry,
            RemoteLockWatchClient remoteLockWatchClient,
            ConflictDetectionManager conflictDetectionManager,
            TransactionService transactionService) {
        this.delegate = delegate;
        this.conflictDetectionManager = conflictDetectionManager;
        this.rowCacheReader = new RowCacheReaderImpl(watchRegistry, remoteLockWatchClient,
                new RowStateCache(delegate, transactionService));
        readCount = new AtomicLong();
    }

    @VisibleForTesting
    public long getReadCount() {
        return readCount.get();
    }

    @VisibleForTesting
    public void resetReadCount() {
        readCount.set(0);
    }

    @VisibleForTesting
    public void flushCache() {
        rowCacheReader.ensureCacheFlushed();
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
        if (!conflictDetectionManager.get(tableRef).lockRowsForConflicts()) {
            return delegate.getRows(tableRef, rows, columnSelection, timestamp);
        }

        Map<Cell, Value> results = Maps.newHashMap();
        RowCacheRowReadAttemptResult<Map<Cell, Value>> cacheRead = rowCacheReader.attemptToRead(
                tableRef,
                rows,
                timestamp,
                values -> KeyedStream.stream(values)
                        .filterKeys(cell -> columnSelection.contains(cell.getColumnName())).collectToMap());
        results.putAll(cacheRead.output());

        List<byte[]> rowsThatMustBeReadFromKvs = StreamSupport.stream(rows.spliterator(), false)
                .filter(t -> !cacheRead.rowsSuccessfullyReadFromCache().contains(t))
                .collect(Collectors.toList());
        Map<Cell, Value> kvsRead = delegate.getRows(tableRef,
                rowsThatMustBeReadFromKvs,
                columnSelection,
                timestamp);
        results.putAll(kvsRead);
        readCount.addAndGet(rowsThatMustBeReadFromKvs.size());

        log.info("Read {} from cache and {} from kvs", cacheRead, kvsRead);

        return results;
    }

    @Override
    public Map<byte[], RowColumnRangeIterator> getRowsColumnRange(TableReference tableRef, Iterable<byte[]> rows,
            BatchColumnRangeSelection batchColumnRangeSelection, long timestamp) {
        if (!conflictDetectionManager.get(tableRef).lockRowsForConflicts()) {
            return delegate.getRowsColumnRange(tableRef, rows, batchColumnRangeSelection, timestamp);
        }

        Map<byte[], RowColumnRangeIterator> results = Maps.newHashMap();
        RowCacheRowReadAttemptResult<Map<byte[], RowColumnRangeIterator>> cacheRead = rowCacheReader.attemptToRead(
                tableRef,
                rows,
                timestamp,
                values -> convertToRowColumnRangeOutputFormat(values));
        results.putAll(cacheRead.output());

        List<byte[]> rowsThatMustBeReadFromKvs = StreamSupport.stream(rows.spliterator(), false)
                .filter(t -> !cacheRead.rowsSuccessfullyReadFromCache().contains(t))
                .collect(Collectors.toList());
        Map<byte[], RowColumnRangeIterator> kvsRead = delegate.getRowsColumnRange(tableRef,
                rowsThatMustBeReadFromKvs,
                batchColumnRangeSelection,
                timestamp);
        results.putAll(kvsRead);
        readCount.addAndGet(rowsThatMustBeReadFromKvs.size());

        log.info("Read {} from cache and {} from kvs", cacheRead, kvsRead);

        return results;
    }

    private Map<byte[], RowColumnRangeIterator> convertToRowColumnRangeOutputFormat(Map<Cell, Value> values) {
        SortedMap<byte[], List<Map.Entry<Cell, Value>>> cellsByRow = values.entrySet()
                .stream()
                .collect(Collectors.groupingBy(entry -> entry.getKey().getRowName(),
                        () -> Maps.newTreeMap(UnsignedBytes.lexicographicalComparator()),
                        Collectors.toList()));
        SortedMap<byte[], RowColumnRangeIterator> result = Maps.newTreeMap(UnsignedBytes.lexicographicalComparator());
        for (Map.Entry<byte[], List<Map.Entry<Cell, Value>>> entry : cellsByRow.entrySet()) {
            result.put(entry.getKey(), new LocalRowColumnRangeIterator(entry.getValue().iterator());
        }
        return result;
    }

    @Override
    public RowColumnRangeIterator getRowsColumnRange(TableReference tableRef, Iterable<byte[]> rows,
            ColumnRangeSelection columnRangeSelection, int cellBatchHint, long timestamp) {
        // Analogous to getRowsColumnRange without the batch hint.
        return delegate.getRowsColumnRange(tableRef, rows, columnRangeSelection, cellBatchHint, timestamp);
    }

    @Override
    public Map<Cell, Value> get(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        // Filter out rows that are possibly cacheable.
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
