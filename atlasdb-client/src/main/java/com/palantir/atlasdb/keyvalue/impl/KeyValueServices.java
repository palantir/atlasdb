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

import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.primitives.UnsignedBytes;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.keyvalue.api.AsyncKeyValueService;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.common.annotation.Output;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.Throwables;
import com.palantir.common.concurrent.BlockingWorkerPool;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.util.crypto.Sha256Hash;
import com.palantir.util.paging.SimpleTokenBackedResultsPage;
import com.palantir.util.paging.TokenBackedBasicResultsPage;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import org.apache.commons.lang3.tuple.Pair;

public final class KeyValueServices {
    private static final SafeLogger log = SafeLoggerFactory.get(KeyValueServices.class);

    private KeyValueServices() {
        /**/
    }

    public static TableMetadata getTableMetadataSafe(KeyValueService service, TableReference tableRef) {
        try {
            byte[] metadataForTable = service.getMetadataForTable(tableRef);
            if (metadataForTable == null || metadataForTable.length == 0) {
                return null;
            }
            return TableMetadata.BYTES_HYDRATOR.hydrateFromBytes(metadataForTable);
        } catch (Exception e) {
            log.warn("failed to get metadata for table", e);
            return null;
        }
    }

    public static void getFirstBatchForRangeUsingGetRange(
            KeyValueService kv,
            TableReference tableRef,
            RangeRequest request,
            long timestamp,
            @Output Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> ret) {
        if (ret.containsKey(request)) {
            return;
        }
        RangeRequest requestWithHint = request;
        if (request.getBatchHint() == null) {
            requestWithHint = request.withBatchHint(100);
        }
        final ClosableIterator<RowResult<Value>> range = kv.getRange(tableRef, requestWithHint, timestamp);
        try {
            int batchSize = requestWithHint.getBatchHint();
            final Iterator<RowResult<Value>> withLimit = Iterators.limit(range, batchSize);
            ImmutableList<RowResult<Value>> results = ImmutableList.copyOf(withLimit);
            if (results.size() != batchSize) {
                ret.put(request, SimpleTokenBackedResultsPage.create(request.getEndExclusive(), results, false));
                return;
            }
            RowResult<Value> last = results.get(results.size() - 1);
            byte[] lastRowName = last.getRowName();
            if (RangeRequests.isTerminalRow(request.isReverse(), lastRowName)) {
                ret.put(request, SimpleTokenBackedResultsPage.create(lastRowName, results, false));
                return;
            }
            byte[] nextStartRow = RangeRequests.getNextStartRow(request.isReverse(), lastRowName);
            if (Arrays.equals(request.getEndExclusive(), nextStartRow)) {
                ret.put(request, SimpleTokenBackedResultsPage.create(nextStartRow, results, false));
            } else {
                ret.put(request, SimpleTokenBackedResultsPage.create(nextStartRow, results, true));
            }
        } finally {
            range.close();
        }
    }

    @SuppressWarnings("checkstyle:LineLength")
    public static Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>>
            getFirstBatchForRangesUsingGetRangeConcurrent(
                    ExecutorService executor,
                    final KeyValueService kv,
                    final TableReference tableRef,
                    Iterable<RangeRequest> rangeRequests,
                    final long timestamp,
                    int maxConcurrentRequests) {
        final Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> ret = new ConcurrentHashMap<>();
        BlockingWorkerPool pool = new BlockingWorkerPool(executor, maxConcurrentRequests);
        try {
            for (final RangeRequest request : rangeRequests) {
                pool.submitTask(() -> getFirstBatchForRangeUsingGetRange(kv, tableRef, request, timestamp, ret));
            }
            pool.waitForSubmittedTasks();
            return ret;
        } catch (InterruptedException e) {
            throw Throwables.rewrapAndThrowUncheckedException(e);
        }
    }

    @SuppressWarnings("checkstyle:LineLength")
    public static Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>>
            getFirstBatchForRangesUsingGetRange(
                    KeyValueService kv, TableReference tableRef, Iterable<RangeRequest> rangeRequests, long timestamp) {
        Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> ret = new HashMap<>();
        for (final RangeRequest request : rangeRequests) {
            getFirstBatchForRangeUsingGetRange(kv, tableRef, request, timestamp, ret);
        }
        return ret;
    }

    public static Collection<Map.Entry<Cell, Value>> toConstantTimestampValues(
            final Collection<Map.Entry<Cell, byte[]>> cells, final long timestamp) {
        return Collections2.transform(
                cells, entry -> Maps.immutableEntry(entry.getKey(), Value.create(entry.getValue(), timestamp)));
    }

    // TODO(gsheasby): kill this when we can properly implement this on all KVSes
    public static Map<byte[], RowColumnRangeIterator> filterGetRowsToColumnRange(
            KeyValueService kvs,
            TableReference tableRef,
            Iterable<byte[]> rows,
            BatchColumnRangeSelection columnRangeSelection,
            long timestamp) {
        log.warn("Using inefficient postfiltering for getRowsColumnRange because the KVS doesn't support it natively. "
                + "Production environments should use a KVS with a proper implementation.");
        Map<Cell, Value> allValues = kvs.getRows(tableRef, rows, ColumnSelection.all(), timestamp);
        Map<Sha256Hash, byte[]> hashesToBytes = new HashMap<>();
        Map<Sha256Hash, ImmutableSortedMap.Builder<byte[], Value>> rowsToColumns = new HashMap<>();

        for (byte[] row : rows) {
            Sha256Hash rowHash = Sha256Hash.computeHash(row);
            hashesToBytes.put(rowHash, row);
            ImmutableSortedMap.Builder<byte[], Value> builder =
                    ImmutableSortedMap.<byte[], Value>orderedBy(UnsignedBytes.lexicographicalComparator());
            rowsToColumns.put(rowHash, builder);
        }
        for (Map.Entry<Cell, Value> e : allValues.entrySet()) {
            Sha256Hash rowHash = Sha256Hash.computeHash(e.getKey().getRowName());
            rowsToColumns.get(rowHash).put(e.getKey().getColumnName(), e.getValue());
        }

        IdentityHashMap<byte[], RowColumnRangeIterator> results = new IdentityHashMap<>();
        for (Map.Entry<Sha256Hash, ImmutableSortedMap.Builder<byte[], Value>> row : rowsToColumns.entrySet()) {
            SortedMap<byte[], Value> map = row.getValue().build();
            Set<Map.Entry<byte[], Value>> subMap;
            if ((columnRangeSelection.getStartCol().length == 0) && (columnRangeSelection.getEndCol().length == 0)) {
                subMap = map.entrySet();
            } else if (columnRangeSelection.getStartCol().length == 0) {
                subMap = map.headMap(columnRangeSelection.getEndCol()).entrySet();
            } else if (columnRangeSelection.getEndCol().length == 0) {
                subMap = map.tailMap(columnRangeSelection.getStartCol()).entrySet();
            } else {
                subMap = map.subMap(columnRangeSelection.getStartCol(), columnRangeSelection.getEndCol())
                        .entrySet();
            }
            byte[] rowName = hashesToBytes.get(row.getKey());
            results.put(
                    hashesToBytes.get(row.getKey()),
                    new LocalRowColumnRangeIterator(Iterators.transform(
                            subMap.iterator(),
                            e -> Pair.<Cell, Value>of(Cell.create(rowName, e.getKey()), e.getValue()))));
        }
        return results;
    }

    public static RowColumnRangeIterator mergeGetRowsColumnRangeIntoSingleIterator(
            KeyValueService kvs,
            TableReference tableRef,
            Iterable<byte[]> rows,
            ColumnRangeSelection columnRangeSelection,
            int batchHint,
            long timestamp) {
        if (Iterables.isEmpty(rows)) {
            return new LocalRowColumnRangeIterator(Collections.emptyIterator());
        }
        int columnBatchSize = Math.max(1, batchHint / Iterables.size(rows));
        BatchColumnRangeSelection batchColumnRangeSelection =
                BatchColumnRangeSelection.create(columnRangeSelection, columnBatchSize);
        Map<byte[], RowColumnRangeIterator> rowsColumnRanges =
                kvs.getRowsColumnRange(tableRef, rows, batchColumnRangeSelection, timestamp);
        // Return results in the same order as the provided rows.
        Iterable<RowColumnRangeIterator> orderedRanges = Iterables.transform(rows, rowsColumnRanges::get);
        return new LocalRowColumnRangeIterator(Iterators.concat(orderedRanges.iterator()));
    }

    /**
     * Constructs an {@link AsyncKeyValueService} such that methods are blocking and return immediate futures.
     *
     * @param keyValueService on which to call synchronous requests
     * @return {@link AsyncKeyValueService} which delegates to synchronous methods
     */
    public static AsyncKeyValueService synchronousAsAsyncKeyValueService(KeyValueService keyValueService) {
        return new AsyncKeyValueService() {
            @Override
            public ListenableFuture<Map<Cell, Value>> getAsync(
                    TableReference tableRef, Map<Cell, Long> timestampByCell) {
                return Futures.immediateFuture(keyValueService.get(tableRef, timestampByCell));
            }

            @Override
            public void close() {
                // NoOp
            }
        };
    }
}
