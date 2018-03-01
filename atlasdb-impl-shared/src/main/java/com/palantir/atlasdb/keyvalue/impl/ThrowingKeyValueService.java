/*
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

import com.google.common.collect.Multimap;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
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
import com.palantir.common.annotation.Idempotent;
import com.palantir.common.annotation.NonIdempotent;
import com.palantir.common.base.ClosableIterator;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

public class ThrowingKeyValueService implements KeyValueService {

    private final String errorMessage;
    private final Exception exception;

    public ThrowingKeyValueService(String errorMessage) {
        this.errorMessage = errorMessage;
        this.exception = new Exception("Provided for stack trace");
    }

    public ThrowingKeyValueService(Exception ex) {
        this.errorMessage = "Unable to create a key value service with the given preferences."
                + " This error was captured and delayed until now when the key value service was used."
                + " Consult the AtlasDB documentation on CHANGING DATABASE CREDENTIALS AND OTHER PARAMETERS.";
        this.exception = ex;
    }

    public IllegalStateException throwEx() {
        throw new IllegalStateException(errorMessage, exception);
    }

    @Override
    public void close() {
        // do nothing
    }

    @Override
    public Collection<? extends KeyValueService> getDelegates() {
        throw throwEx();
    }

    @Override
    @Idempotent
    public Map<Cell, Value> getRows(TableReference tableRef,
                                    Iterable<byte[]> rows,
                                    ColumnSelection columnSelection,
                                    long timestamp) {
        throw throwEx();
    }

    @Override
    @Idempotent
    public Map<Cell, Value> get(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        throw throwEx();
    }

    @Override
    @Idempotent
    public Map<Cell, Long> getLatestTimestamps(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        throw throwEx();
    }

    @Override
    @NonIdempotent
    public void put(TableReference tableRef, Map<Cell, byte[]> values, long timestamp) {
        throw throwEx();
    }

    @Override
    @NonIdempotent
    public void multiPut(Map<TableReference, ? extends Map<Cell, byte[]>> valuesByTable, long timestamp) {
        throw throwEx();
    }

    @Override
    @NonIdempotent
    public void putWithTimestamps(TableReference tableRef, Multimap<Cell, Value> values) {
        throw throwEx();
    }

    @Override
    public void putUnlessExists(TableReference tableRef, Map<Cell, byte[]> values) throws KeyAlreadyExistsException {
        throw throwEx();
    }

    @Override
    public boolean supportsCheckAndSet() {
        throw throwEx();
    }

    @Override
    public void checkAndSet(CheckAndSetRequest checkAndSetRequest) {
        throw throwEx();
    }

    @Override
    @Idempotent
    public void delete(TableReference tableRef, Multimap<Cell, Long> keys) {
        throw throwEx();
    }

    @Override
    @Idempotent
    public void deleteRange(TableReference tableRef, RangeRequest range) {
        throw throwEx();
    }

    @Override
    @Idempotent
    public void truncateTable(TableReference tableRef) {
        throw throwEx();
    }

    @Override
    @Idempotent
    public void truncateTables(Set<TableReference> tableRefs) {
        throw throwEx();
    }

    @Override
    @Idempotent
    public ClosableIterator<RowResult<Value>> getRange(TableReference tableRef,
                                                       RangeRequest rangeRequest,
                                                       long timestamp) {
        throw throwEx();
    }

    @Override
    public ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(TableReference tableRef,
                                                                       RangeRequest rangeRequest,
                                                                       long timestamp) {
        throw throwEx();
    }

    @Override
    @Idempotent
    public Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(
            TableReference tableRef,
            Iterable<RangeRequest> rangeRequests,
            long timestamp) {
        throw throwEx();
    }

    @Override
    @Idempotent
    public void dropTable(TableReference tableRef) {
        throw throwEx();
    }

    @Override
    @Idempotent
    public void dropTables(Set<TableReference> tableRefs) {
        throw throwEx();
    }

    @Override
    @Idempotent
    public void createTable(TableReference tableRef, byte[] tableMetadata) {
        throw throwEx();
    }

    @Override
    @Idempotent
    public Set<TableReference> getAllTableNames() {
        throw throwEx();
    }

    @Override
    @Idempotent
    public byte[] getMetadataForTable(TableReference tableRef) {
        throw throwEx();
    }

    @Override
    @Idempotent
    public Map<TableReference, byte[]> getMetadataForTables() {
        throw throwEx();
    }

    @Override
    @Idempotent
    public void putMetadataForTable(TableReference tableRef, byte[] metadata) {
        throw throwEx();
    }

    @Override
    @Idempotent
    public void addGarbageCollectionSentinelValues(TableReference tableRef, Set<Cell> cells) {
        throw throwEx();
    }

    @Override
    @Idempotent
    public Multimap<Cell, Long> getAllTimestamps(TableReference tableRef, Set<Cell> cells, long timestamp) {
        throw throwEx();
    }

    @Override
    public void createTables(Map<TableReference, byte[]> tableRefToTableMetadata) {
        throw throwEx();
    }

    @Override
    public void putMetadataForTables(Map<TableReference, byte[]> tableRefToMetadata) {
        throw throwEx();
    }

    @Override
    public void compactInternally(TableReference tableRef) {
        throw throwEx();
    }

    @Override
    public Map<byte[], RowColumnRangeIterator> getRowsColumnRange(
            TableReference tableRef,
            Iterable<byte[]> rows,
            BatchColumnRangeSelection batchColumnRangeSelection,
            long timestamp) {
        throw throwEx();
    }

    @Override
    public RowColumnRangeIterator getRowsColumnRange(TableReference tableRef,
                                                     Iterable<byte[]> rows,
                                                     ColumnRangeSelection columnRangeSelection,
                                                     int cellBatchHint,
                                                     long timestamp) {
        throw throwEx();
    }

    @Override
    public boolean shouldTriggerCompactions() {
        throw throwEx();
    }
}
