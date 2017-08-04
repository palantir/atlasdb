/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.deepkin;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.function.Supplier;

import com.google.common.base.Throwables;
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
import com.palantir.common.annotation.Idempotent;
import com.palantir.common.base.BatchingVisitable;
import com.palantir.remoting2.tracing.OpenSpan;
import com.palantir.remoting2.tracing.SpanType;
import com.palantir.remoting2.tracing.Tracer;

import okio.Buffer;

public class DeepkinTransaction implements Transaction {
    private final Transaction delegate;

    public DeepkinTransaction(Transaction delegate) {
        this.delegate = delegate;
    }

    @Override
    @Idempotent
    public SortedMap<byte[], RowResult<byte[]>> getRows(
            TableReference tableRef, Iterable<byte[]> rows,
            ColumnSelection columnSelection) {
        return delegate(
                TransactionMethod.GET_ROWS,
                () -> delegate.getRows(tableRef, rows, columnSelection),
                tableRef, rows, columnSelection
        );
    }

    @Override
    @Idempotent
    public Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> getRowsColumnRange(
            TableReference tableRef, Iterable<byte[]> rows,
            BatchColumnRangeSelection columnRangeSelection) {
        return delegate(
                TransactionMethod.GET_ROWS_COLUMN_RANGE,
                () -> delegate.getRowsColumnRange(tableRef, rows, columnRangeSelection),
                tableRef, rows, columnRangeSelection
        );
    }

    @Override
    @Idempotent
    public Iterator<Map.Entry<Cell, byte[]>> getRowsColumnRange(
            TableReference tableRef, Iterable<byte[]> rows,
            ColumnRangeSelection columnRangeSelection, int batchHint) {
        return delegate(
                TransactionMethod.GET_BATCHED_ROWS_COLUMN_RANGE,
                () -> delegate.getRowsColumnRange(tableRef, rows, columnRangeSelection, batchHint),
                tableRef, rows, columnRangeSelection, batchHint
        );
    }

    @Override
    @Idempotent
    public Map<Cell, byte[]> get(
            TableReference tableRef,
            Set<Cell> cells) {
        return delegate(TransactionMethod.GET, () -> delegate.get(tableRef, cells), tableRef, cells);
    }

    @Override
    @Idempotent
    public BatchingVisitable<RowResult<byte[]>> getRange(
            TableReference tableRef,
            RangeRequest rangeRequest) {
        return delegate(TransactionMethod.GET_RANGE, () -> delegate.getRange(tableRef, rangeRequest), tableRef, rangeRequest);
    }

    @Override
    @Idempotent
    public Iterable<BatchingVisitable<RowResult<byte[]>>> getRanges(
            TableReference tableRef,
            Iterable<RangeRequest> rangeRequests) {
        return delegate(TransactionMethod.GET_RANGES, () -> delegate.getRanges(tableRef, rangeRequests), tableRef, rangeRequests);
    }

    @Override
    @Idempotent
    public void commit() throws TransactionFailedException {
        TransactionFailedException failure = delegate(TransactionMethod.COMMIT, () -> {
            try {
                delegate.commit();
            } catch (TransactionFailedException e) {
                return e;
            }
            return null;
        });
        if (failure != null) {
            throw failure;
        }
    }

    @Override
    @Idempotent
    public void commit(TransactionService transactionService) throws TransactionFailedException {
        TransactionFailedException failure = delegate(TransactionMethod.COMMIT_SERVICE, () -> {
            try {
                delegate.commit(transactionService);
            } catch (TransactionFailedException e) {
                return e;
            }
            return null;
        });
        if (failure != null) {
            throw failure;
        }
    }

    @Override
    @Idempotent
    public long getTimestamp() {
        return delegate(TransactionMethod.GET_TIMESTAMP, delegate::getTimestamp);
    }

    private <T, Y> T delegate(TransactionMethod<Y, T> call, Supplier<T> resulter, Object... arguments) {
        if (!Tracer.isTraceObservable()) {
            return resulter.get();
        }
        OpenSpan span = Tracer.startSpan(call.name(), SpanType.CLIENT_OUTGOING);
        span.getRequestBuffer().ifPresent(buffer -> unsafeBufferWrite(buffer, call.transformArguments(arguments)));
        T result = resulter.get();
        Y serialized = call.resultTransform().serializer().apply(result);
        span.getResponseBuffer().ifPresent(buffer -> unsafeBufferWrite(buffer, serialized));
        Tracer.completeSpan();
        return call.resultTransform().deserializer().apply(serialized);
    }

    private void unsafeBufferWrite(Buffer buffer, Object object) {
        try {
            new ObjectOutputStream(buffer.outputStream()).writeObject(object);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    @Idempotent
    public void put(TableReference tableRef,
            Map<Cell, byte[]> values) {
        delegate.put(tableRef, values);
    }

    @Override
    @Idempotent
    public void delete(TableReference tableRef,
            Set<Cell> keys) {
        delegate.delete(tableRef, keys);
    }

    @Override
    @Idempotent
    public TransactionType getTransactionType() {
        return delegate.getTransactionType();
    }

    @Override
    @Idempotent
    public void setTransactionType(TransactionType transactionType) {
        delegate.setTransactionType(transactionType);
    }

    @Override
    @Idempotent
    public void abort() {
        delegate.abort();
    }

    @Override
    @Idempotent
    public boolean isAborted() {
        return delegate.isAborted();
    }

    @Override
    @Idempotent
    public boolean isUncommitted() {
        return delegate.isUncommitted();
    }

    @Override
    public TransactionReadSentinelBehavior getReadSentinelBehavior() {
        return delegate.getReadSentinelBehavior();
    }

    @Override
    public void useTable(TableReference tableRef,
            ConstraintCheckable table) {
        delegate.useTable(tableRef, table);
    }
}
