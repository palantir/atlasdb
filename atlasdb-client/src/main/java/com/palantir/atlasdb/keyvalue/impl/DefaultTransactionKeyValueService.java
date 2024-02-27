/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.MustBeClosed;
import com.palantir.atlasdb.cell.api.TransactionKeyValueService;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
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
import java.util.Map;
import java.util.Optional;

public final class DefaultTransactionKeyValueService implements TransactionKeyValueService {

    private final KeyValueService delegate;

    public DefaultTransactionKeyValueService(KeyValueService delegate) {
        this.delegate = delegate;
    }

    @Override
    public Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(
            TableReference tableRef, Iterable<RangeRequest> rangeRequests, long timestamp) {
        return delegate.getFirstBatchForRanges(tableRef, rangeRequests, timestamp);
    }

    @Override
    public Map<Cell, Value> getRows(
            TableReference tableRef, Iterable<byte[]> rows, ColumnSelection columnSelection, long timestamp) {
        return delegate.getRows(tableRef, rows, columnSelection, timestamp);
    }

    @Override
    public Map<byte[], RowColumnRangeIterator> getRowsColumnRange(
            TableReference tableRef,
            Iterable<byte[]> rows,
            BatchColumnRangeSelection batchColumnRangeSelection,
            long timestamp) {
        return delegate.getRowsColumnRange(tableRef, rows, batchColumnRangeSelection, timestamp);
    }

    @Override
    public RowColumnRangeIterator getRowsColumnRange(
            TableReference tableRef,
            Iterable<byte[]> rows,
            ColumnRangeSelection columnRangeSelection,
            int cellBatchHint,
            long timestamp) {
        return delegate.getRowsColumnRange(tableRef, rows, columnRangeSelection, cellBatchHint, timestamp);
    }

    @MustBeClosed
    @Override
    public ClosableIterator<RowResult<Value>> getRange(
            TableReference tableRef, RangeRequest rangeRequest, long timestamp) {
        return delegate.getRange(tableRef, rangeRequest, timestamp);
    }

    @Override
    public Map<Cell, Value> get(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        return delegate.get(tableRef, timestampByCell);
    }

    @Override
    public ListenableFuture<Map<Cell, Value>> getAsync(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        return delegate.getAsync(tableRef, timestampByCell);
    }

    @Override
    public Map<Cell, Long> getLatestTimestamps(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        return delegate.getLatestTimestamps(tableRef, timestampByCell);
    }

    @Override
    public void multiPut(Map<TableReference, ? extends Map<Cell, byte[]>> valuesByTable, long timestamp)
            throws KeyAlreadyExistsException {
        delegate.multiPut(valuesByTable, timestamp);
    }

    @Override
    public Optional<TransactionKeyValueService> maybeValidationReadTarget(TableReference tableRef) {
        return delegate.maybeValidationReadTarget(tableRef);
    }
}
