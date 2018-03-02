/*
 * Copyright 2016 Palantir Technologies
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

import com.google.common.base.Preconditions;
import com.google.common.collect.ForwardingObject;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.tracing.CloseableTrace;
import com.palantir.common.base.ClosableIterator;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

/**
 * Wraps a {@link KeyValueService}'s methods with {@link com.palantir.remoting1.tracing.Tracer}
 * instrumentation.
 */
public final class TracingKeyValueService extends ForwardingObject implements KeyValueService {

    private static final String SERVICE_NAME = "atlasdb-kvs";

    private final KeyValueService delegate;

    private TracingKeyValueService(KeyValueService delegate) {
        this.delegate = Preconditions.checkNotNull(delegate, "delegate");
    }

    public static KeyValueService create(KeyValueService keyValueService) {
        return new TracingKeyValueService(keyValueService);
    }

    private static CloseableTrace startLocalTrace(CharSequence operationFormat, Object... formatArguments) {
        return CloseableTrace.startLocalTrace(SERVICE_NAME, operationFormat, formatArguments);
    }

    @Override
    protected KeyValueService delegate() {
        return delegate;
    }

    @Override
    public void addGarbageCollectionSentinelValues(TableReference tableRef, Set<Cell> cells) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace = startLocalTrace("addGarbageCollectionSentinelValues({}, {} cells)",
                tableRef, cells.size())) {
            delegate().addGarbageCollectionSentinelValues(tableRef, cells);
        }
    }

    @Override
    public void checkAndSet(CheckAndSetRequest checkAndSetRequest) throws CheckAndSetException {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace = startLocalTrace("checkAndSet({}, {})",
                checkAndSetRequest.table(), checkAndSetRequest.cell())) {
            delegate().checkAndSet(checkAndSetRequest);
        }
    }

    @Override
    public void close() {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace = startLocalTrace("close()")) {
            delegate().close();
        }
    }

    @Override
    public void compactInternally(TableReference tableRef) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace = startLocalTrace("compactInternally({})", tableRef)) {
            delegate().compactInternally(tableRef);
        }
    }

    @Override
    public void compactInternally(TableReference tableRef, boolean inSafeHours) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace = startLocalTrace("compactInternally({})", tableRef)) {
            delegate().compactInternally(tableRef, inSafeHours);
        }
    }

    @Override
    public void createTable(TableReference tableRef, byte[] tableMetadata) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace = startLocalTrace("createTable({})", tableRef)) {
            delegate().createTable(tableRef, tableMetadata);
        }
    }

    @Override
    public void createTables(Map<TableReference, byte[]> tableNamesToTableMetadata) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace = startLocalTrace("createTables({})",
                tableNamesToTableMetadata.keySet())) {
            delegate().createTables(tableNamesToTableMetadata);
        }
    }

    @Override
    public void delete(TableReference tableRef, Multimap<Cell, Long> keys) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace = startLocalTrace("delete({}, {} keys)", tableRef, keys.size())) {
            delegate().delete(tableRef, keys);
        }
    }

    @Override
    public void deleteRange(TableReference tableRef, RangeRequest range) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace = startLocalTrace("deleteRange({})", tableRef)) {
            delegate().deleteRange(tableRef, range);
        }
    }

    @Override
    public void dropTable(TableReference tableRef) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace = startLocalTrace("dropTable({})", tableRef)) {
            delegate().dropTable(tableRef);
        }
    }

    @Override
    public void dropTables(Set<TableReference> tableRefs) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace = startLocalTrace("dropTables({})", tableRefs)) {
            delegate().dropTables(tableRefs);
        }
    }

    @Override
    public Map<Cell, Value> get(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace = startLocalTrace("get({}, {} cells)",
                tableRef, timestampByCell.size())) {
            return delegate().get(tableRef, timestampByCell);
        }
    }

    @Override
    public Set<TableReference> getAllTableNames() {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace = startLocalTrace("getAllTableNames()")) {
            return delegate().getAllTableNames();
        }
    }

    @Override
    public Multimap<Cell, Long> getAllTimestamps(TableReference tableRef, Set<Cell> keys, long timestamp) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace = startLocalTrace("getAllTimestamps({}, {} keys, ts {})",
                tableRef, keys.size(), timestamp)) {
            return delegate().getAllTimestamps(tableRef, keys, timestamp);
        }
    }

    @Override
    public Collection<? extends KeyValueService> getDelegates() {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace = startLocalTrace("getDelegates()")) {
            return ImmutableList.copyOf(Iterables.concat(ImmutableList.of(delegate()), delegate().getDelegates()));
        }
    }

    @Override
    public Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(
            TableReference tableRef,
            Iterable<RangeRequest> rangeRequests,
            long timestamp) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace = startLocalTrace("getFirstBatchForRanges({}, {} ranges, ts {})",
                tableRef, Iterables.size(rangeRequests), timestamp)) {
            return delegate().getFirstBatchForRanges(tableRef, rangeRequests, timestamp);
        }
    }

    @Override
    public Map<Cell, Long> getLatestTimestamps(TableReference tableRef,
            Map<Cell, Long> timestampByCell) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace = startLocalTrace("getLatestTimestamps({}, {} cells)",
                tableRef, timestampByCell.size())) {
            return delegate().getLatestTimestamps(tableRef, timestampByCell);
        }
    }

    @Override
    public byte[] getMetadataForTable(TableReference tableRef) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace = startLocalTrace("getMetadataForTable({})", tableRef)) {
            return delegate().getMetadataForTable(tableRef);
        }
    }

    @Override
    public Map<TableReference, byte[]> getMetadataForTables() {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace = startLocalTrace("getMetadataForTables()")) {
            return delegate().getMetadataForTables();
        }
    }

    @Override
    public ClosableIterator<RowResult<Value>> getRange(TableReference tableRef,
            RangeRequest rangeRequest,
            long timestamp) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace = startLocalTrace("getRange({}, ts {})",
                tableRef, timestamp)) {
            return delegate().getRange(tableRef, rangeRequest, timestamp);
        }
    }

    @Override
    public ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(TableReference tableRef,
            RangeRequest rangeRequest,
            long timestamp) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace = startLocalTrace("getRangeOfTimestamps({}, ts {})",
                tableRef, timestamp)) {
            return delegate().getRangeOfTimestamps(tableRef, rangeRequest, timestamp);
        }
    }

    @Override
    public Map<Cell, Value> getRows(TableReference tableRef,
            Iterable<byte[]> rows,
            ColumnSelection columnSelection,
            long timestamp) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace = startLocalTrace("getRows({}, {} rows, ts {})",
                tableRef, Iterables.size(rows), timestamp)) {
            return delegate().getRows(tableRef, rows, columnSelection, timestamp);
        }
    }

    @Override
    public Map<byte[], RowColumnRangeIterator> getRowsColumnRange(TableReference tableRef,
            Iterable<byte[]> rows,
            BatchColumnRangeSelection columnRangeSelection,
            long timestamp) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace = startLocalTrace("getRowsColumnRange({}, {} rows, ts {})",
                tableRef, Iterables.size(rows), timestamp)) {
            return delegate().getRowsColumnRange(tableRef, rows, columnRangeSelection, timestamp);
        }
    }

    @Override
    public RowColumnRangeIterator getRowsColumnRange(TableReference tableRef,
            Iterable<byte[]> rows,
            ColumnRangeSelection columnRangeSelection,
            int cellBatchHint,
            long timestamp) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace = startLocalTrace("getRowsColumnRange({}, {} rows, {} hint, ts {})",
                tableRef, Iterables.size(rows), cellBatchHint, timestamp)) {
            return delegate().getRowsColumnRange(tableRef, rows, columnRangeSelection, cellBatchHint, timestamp);
        }
    }

    @Override
    public void multiPut(Map<TableReference, ? extends Map<Cell, byte[]>> valuesByTable,
            long timestamp) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace = startLocalTrace("multiPut({} values, ts {})",
                valuesByTable.size(), timestamp)) {
            delegate().multiPut(valuesByTable, timestamp);
        }
    }

    @Override
    public void put(TableReference tableRef, Map<Cell, byte[]> values, long timestamp) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace = startLocalTrace("put({}, {} values, ts {})",
                tableRef, values.size(), timestamp)) {
            delegate().put(tableRef, values, timestamp);
        }
    }

    @Override
    public void putMetadataForTable(TableReference tableRef, byte[] metadata) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace = startLocalTrace("putMetadataForTable({}, {} bytes)",
                tableRef, (metadata == null) ? 0 : metadata.length)) {
            delegate().putMetadataForTable(tableRef, metadata);
        }
    }

    @Override
    public void putMetadataForTables(Map<TableReference, byte[]> tableRefToMetadata) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace = startLocalTrace("putMetadataForTables({})", tableRefToMetadata.keySet())) {
            delegate().putMetadataForTables(tableRefToMetadata);
        }
    }

    @Override
    public void putUnlessExists(TableReference tableRef, Map<Cell, byte[]> values)
            throws KeyAlreadyExistsException {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace = startLocalTrace("putUnlessExists({}, {} values)",
                tableRef, values.size())) {
            delegate().putUnlessExists(tableRef, values);
        }
    }

    @Override
    public boolean supportsCheckAndSet() {
        return delegate().supportsCheckAndSet();
    }

    @Override
    public void putWithTimestamps(TableReference tableRef, Multimap<Cell, Value> values) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace = startLocalTrace(
                "putWithTimestamps({}, {} values)", tableRef, values.size())) {
            delegate().putWithTimestamps(tableRef, values);
        }
    }

    @Override
    public void truncateTable(TableReference tableRef) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace = startLocalTrace("truncateTable({})", tableRef)) {
            delegate().truncateTable(tableRef);
        }
    }

    @Override
    public void truncateTables(Set<TableReference> tableRefs) {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace = startLocalTrace("truncateTables({})", tableRefs)) {
            delegate().truncateTables(tableRefs);
        }
    }

    @Override
    public boolean shouldTriggerCompactions() {
        return delegate().shouldTriggerCompactions();
    }
}

