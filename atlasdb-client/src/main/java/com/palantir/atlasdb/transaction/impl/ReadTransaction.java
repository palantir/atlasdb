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
package com.palantir.atlasdb.transaction.impl;

import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.table.description.SweepStrategy;
import com.palantir.atlasdb.transaction.api.GetRangesQuery;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.common.base.BatchingVisitable;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Stream;

public class ReadTransaction extends ForwardingTransaction {
    private final AbstractTransaction delegate;
    private final SweepStrategyManager sweepStrategies;

    public ReadTransaction(AbstractTransaction delegate, SweepStrategyManager sweepStrategies) {
        this.delegate = delegate;
        this.sweepStrategies = sweepStrategies;
    }

    @Override
    public Transaction delegate() {
        return delegate;
    }

    @Override
    public NavigableMap<byte[], RowResult<byte[]>> getRows(
            TableReference tableRef, Iterable<byte[]> rows, ColumnSelection columnSelection) {
        checkTableName(tableRef);
        return delegate().getRows(tableRef, rows, columnSelection);
    }

    @Override
    public Map<Cell, byte[]> get(TableReference tableRef, Set<Cell> cells) {
        checkTableName(tableRef);
        return delegate().get(tableRef, cells);
    }

    @Override
    public BatchingVisitable<RowResult<byte[]>> getRange(TableReference tableRef, RangeRequest rangeRequest) {
        checkTableName(tableRef);
        return delegate().getRange(tableRef, rangeRequest);
    }

    @Override
    public Iterable<BatchingVisitable<RowResult<byte[]>>> getRanges(
            TableReference tableRef, Iterable<RangeRequest> rangeRequests) {
        checkTableName(tableRef);
        return delegate().getRanges(tableRef, rangeRequests);
    }

    @Override
    public <T> Stream<T> getRanges(
            final TableReference tableRef,
            Iterable<RangeRequest> rangeRequests,
            int concurrencyLevel,
            BiFunction<RangeRequest, BatchingVisitable<RowResult<byte[]>>, T> visitableProcessor) {
        checkTableName(tableRef);
        return delegate().getRanges(tableRef, rangeRequests, concurrencyLevel, visitableProcessor);
    }

    @Override
    public <T> Stream<T> getRanges(
            final TableReference tableRef,
            Iterable<RangeRequest> rangeRequests,
            BiFunction<RangeRequest, BatchingVisitable<RowResult<byte[]>>, T> visitableProcessor) {
        checkTableName(tableRef);
        return delegate().getRanges(tableRef, rangeRequests, visitableProcessor);
    }

    @Override
    public <T> Stream<T> getRanges(GetRangesQuery<T> getRangesQuery) {
        return delegate().getRanges(getRangesQuery);
    }

    @Override
    public Stream<BatchingVisitable<RowResult<byte[]>>> getRangesLazy(
            final TableReference tableRef, Iterable<RangeRequest> rangeRequests) {
        checkTableName(tableRef);
        return delegate().getRangesLazy(tableRef, rangeRequests);
    }

    @Override
    public Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> getRowsColumnRange(
            TableReference tableRef, Iterable<byte[]> rows, BatchColumnRangeSelection columnRangeSelection) {
        checkTableName(tableRef);
        return delegate().getRowsColumnRange(tableRef, rows, columnRangeSelection);
    }

    @Override
    public Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> getRowsColumnRangeIterator(
            TableReference tableRef, Iterable<byte[]> rows, BatchColumnRangeSelection columnRangeSelection) {
        checkTableName(tableRef);
        return delegate().getRowsColumnRangeIterator(tableRef, rows, columnRangeSelection);
    }

    @Override
    public Iterator<Map.Entry<Cell, byte[]>> getRowsColumnRange(
            TableReference tableRef, Iterable<byte[]> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
        checkTableName(tableRef);
        return delegate().getRowsColumnRange(tableRef, rows, columnRangeSelection, batchHint);
    }

    private void checkTableName(TableReference tableRef) {
        SweepStrategy sweepStrategy = sweepStrategies.get(tableRef);
        Preconditions.checkState(
                !sweepStrategy.mustCheckImmutableLockAfterReads(),
                "This table cannot be read from a read-only transaction, because its "
                        + "sweep strategy is neither NOTHING nor CONSERVATIVE",
                LoggingArgs.tableRef(tableRef));
    }

    @Override
    public void put(TableReference tableRef, Map<Cell, byte[]> values) {
        throw new SafeIllegalArgumentException("This is a read only transaction.");
    }

    @Override
    public void delete(TableReference tableRef, Set<Cell> keys) {
        throw new SafeIllegalArgumentException("This is a read only transaction.");
    }

    @Override
    public ListenableFuture<Map<Cell, byte[]>> getAsync(TableReference tableRef, Set<Cell> cells) {
        checkTableName(tableRef);
        return delegate().getAsync(tableRef, cells);
    }
}
