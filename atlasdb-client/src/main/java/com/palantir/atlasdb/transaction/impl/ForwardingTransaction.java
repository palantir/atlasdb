/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.transaction.impl;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import com.google.common.collect.ForwardingObject;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.ConstraintCheckable;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionFailedException;
import com.palantir.atlasdb.transaction.api.TransactionReadSentinelBehavior;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.common.base.BatchingVisitable;

public abstract class ForwardingTransaction extends ForwardingObject implements Transaction {

    @Override
    public void useTable(TableReference tableRef, ConstraintCheckable table) {
        delegate().useTable(tableRef, table);
    }

    @Override
    public abstract Transaction delegate();

    @Override
    public SortedMap<byte[], RowResult<byte[]>> getRows(TableReference tableRef,
                                                        Iterable<byte[]> rows,
                                                        ColumnSelection columnSelection) {
        return delegate().getRows(tableRef, rows, columnSelection);
    }

    @Override
    public Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> getRowsColumnRange(TableReference tableRef,
            Iterable<byte[]> rows,
            BatchColumnRangeSelection columnRangeSelection) {
        return delegate().getRowsColumnRange(tableRef, rows, columnRangeSelection);
    }

    @Override
    public Iterator<Entry<Cell, byte[]>> getRowsColumnRange(TableReference tableRef,
                                                            Iterable<byte[]> rows,
                                                            ColumnRangeSelection columnRangeSelection,
                                                            int batchHint) {
        return delegate().getRowsColumnRange(tableRef, rows, columnRangeSelection, batchHint);
    }

    @Override
    public Map<Cell, byte[]> get(TableReference tableRef, Set<Cell> cells) {
        return delegate().get(tableRef, cells);
    }

    @Override
    public BatchingVisitable<RowResult<byte[]>> getRange(TableReference tableRef, RangeRequest rangeRequest) {
        return delegate().getRange(tableRef, rangeRequest);
    }

    @Override
    public Iterable<BatchingVisitable<RowResult<byte[]>>> getRanges(TableReference tableRef,
                                                                    Iterable<RangeRequest> rangeRequests) {
        return delegate().getRanges(tableRef, rangeRequests);
    }

    @Override
    public <T> Stream<T> getRanges(
            final TableReference tableRef,
            Iterable<RangeRequest> rangeRequests,
            int concurrencyLevel,
            BiFunction<RangeRequest, BatchingVisitable<RowResult<byte[]>>, T> visitableProcessor) {
        return delegate().getRanges(tableRef, rangeRequests, concurrencyLevel, visitableProcessor);
    }

    @Override
    public Stream<BatchingVisitable<RowResult<byte[]>>> getRangesLazy(
            final TableReference tableRef, Iterable<RangeRequest> rangeRequests) {
        return delegate().getRangesLazy(tableRef, rangeRequests);
    }

    @Override
    public void put(TableReference tableRef, Map<Cell, byte[]> values) {
        delegate().put(tableRef, values);
    }

    @Override
    public void delete(TableReference tableRef, Set<Cell> keys) {
        delegate().delete(tableRef, keys);
    }

    @Override
    public void abort() {
        delegate().abort();
    }

    @Override
    public void commit() throws TransactionFailedException {
        delegate().commit();
    }

    @Override
    public void commit(TransactionService txService) throws TransactionFailedException {
        delegate().commit(txService);
    }

    @Override
    public boolean isAborted() {
        return delegate().isAborted();
    }

    @Override
    public boolean isUncommitted() {
        return delegate().isUncommitted();
    }

    @Override
    public long getTimestamp() {
        return delegate().getTimestamp();
    }

    @Override
    public TransactionReadSentinelBehavior getReadSentinelBehavior() {
        return delegate().getReadSentinelBehavior();
    }

    @Override
    public void setTransactionType(TransactionType transactionType) {
        delegate().setTransactionType(transactionType);
    }

    @Override
    public TransactionType getTransactionType() {
        return delegate().getTransactionType();
    }
}
