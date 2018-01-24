/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;
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

public class InvocationCapturingTransaction extends RawTransaction {
    private final ConcurrentLinkedQueue<Consumer<Transaction>> invocations = new ConcurrentLinkedQueue<>();

    public InvocationCapturingTransaction(RawTransaction delegate) {
        super(delegate.delegate(), delegate.getImmutableTsLock());
    }

    @Override
    public void useTable(TableReference tableRef, ConstraintCheckable table) {
        invocations.add(transaction -> transaction.useTable(tableRef, table));
        super.useTable(tableRef, table);
    }

    @Override
    public SortedMap<byte[], RowResult<byte[]>> getRows(TableReference tableRef, Iterable<byte[]> rows,
            ColumnSelection columnSelection) {
        invocations.add(transaction -> transaction.getRows(tableRef, rows, columnSelection));
        return super.getRows(tableRef, rows, columnSelection);
    }

    @Override
    public Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> getRowsColumnRange(TableReference tableRef,
            Iterable<byte[]> rows, BatchColumnRangeSelection columnRangeSelection) {
        invocations.add(transaction -> transaction.getRowsColumnRange(tableRef, rows, columnRangeSelection));
        return super.getRowsColumnRange(tableRef, rows, columnRangeSelection);
    }

    @Override
    public Iterator<Map.Entry<Cell, byte[]>> getRowsColumnRange(TableReference tableRef, Iterable<byte[]> rows,
            ColumnRangeSelection columnRangeSelection, int batchHint) {
        invocations.add(transaction -> transaction.getRowsColumnRange(tableRef, rows, columnRangeSelection, batchHint));
        return super.getRowsColumnRange(tableRef, rows, columnRangeSelection, batchHint);
    }

    @Override
    public Map<Cell, byte[]> get(TableReference tableRef, Set<Cell> cells) {
        invocations.add(transaction -> transaction.get(tableRef, cells));
        return super.get(tableRef, cells);
    }

    @Override
    public BatchingVisitable<RowResult<byte[]>> getRange(TableReference tableRef, RangeRequest rangeRequest) {
        invocations.add(transaction -> transaction.getRange(tableRef, rangeRequest));
        return super.getRange(tableRef, rangeRequest);
    }

    @Override
    public Iterable<BatchingVisitable<RowResult<byte[]>>> getRanges(TableReference tableRef,
            Iterable<RangeRequest> rangeRequests) {
        invocations.add(transaction -> transaction.getRanges(tableRef, rangeRequests));
        return super.getRanges(tableRef, rangeRequests);
    }

    @Override
    public <T> Stream<T> getRanges(TableReference tableRef, Iterable<RangeRequest> rangeRequests,
            int concurrencyLevel,
            BiFunction<RangeRequest, BatchingVisitable<RowResult<byte[]>>, T> visitableProcessor) {
        invocations.add(transaction -> transaction.getRanges(tableRef, rangeRequests, concurrencyLevel, visitableProcessor));
        return super.getRanges(tableRef, rangeRequests, concurrencyLevel, visitableProcessor);
    }

    @Override
    public <T> Stream<T> getRanges(TableReference tableRef, Iterable<RangeRequest> rangeRequests,
            BiFunction<RangeRequest, BatchingVisitable<RowResult<byte[]>>, T> visitableProcessor) {
        invocations.add(transaction -> transaction.getRanges(tableRef, rangeRequests, visitableProcessor));
        return super.getRanges(tableRef, rangeRequests, visitableProcessor);
    }

    @Override
    public Stream<BatchingVisitable<RowResult<byte[]>>> getRangesLazy(TableReference tableRef,
            Iterable<RangeRequest> rangeRequests) {
        invocations.add(transaction -> transaction.getRangesLazy(tableRef, rangeRequests));
        return super.getRangesLazy(tableRef, rangeRequests);
    }

    @Override
    public void put(TableReference tableRef, Map<Cell, byte[]> values) {
        invocations.add(transaction -> transaction.put(tableRef, values));
        super.put(tableRef, values);
    }

    @Override
    public void delete(TableReference tableRef, Set<Cell> keys) {
        invocations.add(transaction -> transaction.delete(tableRef, keys));
        super.delete(tableRef, keys);
    }

    @Override
    public void abort() {
        invocations.add(Transaction::abort);
        super.abort();
    }

    @Override
    public void commit() throws TransactionFailedException {
        invocations.add(Transaction::commit);
        super.commit();
    }

    @Override
    public void commit(TransactionService txService) throws TransactionFailedException {
        invocations.add(transaction -> transaction.commit(txService));
        super.commit(txService);
    }

    @Override
    public boolean isAborted() {
        invocations.add(Transaction::isAborted);
        return super.isAborted();
    }

    @Override
    public boolean isUncommitted() {
        invocations.add(Transaction::isUncommitted);
        return super.isUncommitted();
    }

    @Override
    public long getTimestamp() {
        invocations.add(Transaction::getTimestamp);
        return super.getTimestamp();
    }

    @Override
    public TransactionReadSentinelBehavior getReadSentinelBehavior() {
        invocations.add(Transaction::getReadSentinelBehavior);
        return super.getReadSentinelBehavior();
    }

    @Override
    public void setTransactionType(TransactionType transactionType) {
        invocations.add(transaction -> transaction.setTransactionType(transactionType));
        super.setTransactionType(transactionType);
    }

    @Override
    public TransactionType getTransactionType() {
        invocations.add(Transaction::getTransactionType);
        return super.getTransactionType();
    }

    public List<Consumer<Transaction>> invocations() {
        return ImmutableList.copyOf(invocations);
    }
}
