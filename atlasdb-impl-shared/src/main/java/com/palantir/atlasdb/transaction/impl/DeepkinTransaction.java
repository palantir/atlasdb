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

package com.palantir.atlasdb.transaction.impl;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.cache.TimestampCache;
import com.palantir.atlasdb.cleaner.Cleaner;
import com.palantir.atlasdb.deepkin.CachedBatchingVisitable;
import com.palantir.atlasdb.deepkin.TransformedBatchingVisitable;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.TransactionFailedException;
import com.palantir.atlasdb.transaction.api.TransactionReadSentinelBehavior;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.common.annotation.Idempotent;
import com.palantir.common.base.BatchingVisitable;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.RemoteLockService;
import com.palantir.remoting2.tracing.OpenSpan;
import com.palantir.remoting2.tracing.SpanType;
import com.palantir.remoting2.tracing.Tracer;
import com.palantir.timestamp.TimestampService;

import okio.Buffer;

public class DeepkinTransaction extends SerializableTransaction {
    public DeepkinTransaction(KeyValueService keyValueService, RemoteLockService remoteLockService,
            TimestampService timestampService,
            TransactionService transactionService, Cleaner cleaner,
            com.google.common.base.Supplier<Long> startTimeStamp,
            ConflictDetectionManager conflictDetectionManager,
            SweepStrategyManager sweepStrategyManager, long immutableTimestamp,
            List<LockRefreshToken> immutableTsLock,
            AtlasDbConstraintCheckingMode constraintCheckingMode, Long transactionTimeoutMillis,
            TransactionReadSentinelBehavior readSentinelBehavior, boolean allowHiddenTableAccess,
            TimestampCache timestampCache) {
        super(keyValueService, remoteLockService, timestampService, transactionService, cleaner, startTimeStamp, conflictDetectionManager,
                sweepStrategyManager, immutableTimestamp, immutableTsLock, constraintCheckingMode,
                transactionTimeoutMillis, readSentinelBehavior, allowHiddenTableAccess, timestampCache);
    }

    @Override
    @Idempotent
    public SortedMap<byte[], RowResult<byte[]>> getRows(
            TableReference tableRef, Iterable<byte[]> rows,
            ColumnSelection columnSelection) {
        Iterable<byte[]> rowsSer = Lists.newLinkedList(rows);
        return delegate("getRows", () -> super.getRows(tableRef, rowsSer, columnSelection), tableRef, rows, columnSelection);
    }

    @Override
    @Idempotent
    public Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> getRowsColumnRange(
            TableReference tableRef, Iterable<byte[]> rows,
            BatchColumnRangeSelection columnRangeSelection) {
        Iterable<byte[]> rowsSer = Lists.newLinkedList(rows);
        return this.<Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>>, Map<byte[], BatchingVisitable<AbstractMap.SimpleEntry<Cell, byte[]>>>> transformingDelegate(
                "getRowsColumnRange",
                () -> super.getRowsColumnRange(tableRef, rowsSer, columnRangeSelection),
                result -> Maps.transformValues(
                        result,
                        v -> (BatchingVisitable<AbstractMap.SimpleEntry<Cell, byte[]>>) CachedBatchingVisitable.cache(
                                new TransformedBatchingVisitable<>(v, AbstractMap.SimpleEntry::new)
                        )),
                s -> Maps.transformValues(
                        s,
                        v -> (BatchingVisitable<Map.Entry<Cell, byte[]>>) new TransformedBatchingVisitable<AbstractMap.SimpleEntry<Cell, byte[]>, Map.Entry<Cell, byte[]>>(
                                v, e -> e
                        )
                ),
                tableRef, rowsSer, columnRangeSelection);
    }

    @Override
    @Idempotent
    public Iterator<Map.Entry<Cell, byte[]>> getRowsColumnRange(
            TableReference tableRef, Iterable<byte[]> rows,
            ColumnRangeSelection columnRangeSelection, int batchHint) {
        Iterable<byte[]> rowsSer = Lists.newLinkedList(rows);
        return this.<Iterator<Map.Entry<Cell, byte[]>>, List<Map.Entry<Cell, byte[]>>> transformingDelegate(
                "getRowsColumnRange",
                () -> super.getRowsColumnRange(tableRef, rowsSer, columnRangeSelection, batchHint),
                result -> Lists.newArrayList(Iterators.<Map.Entry<Cell, byte[]>, Map.Entry<Cell, byte[]>>transform(
                        result,
                        AbstractMap.SimpleEntry::new)
                ),
                List::iterator,
                tableRef, rowsSer, columnRangeSelection, batchHint);
    }

    @Override
    @Idempotent
    public Map<Cell, byte[]> get(
            TableReference tableRef,
            Set<Cell> cells) {
        return delegate("get", () -> super.get(tableRef, cells), tableRef, cells);
    }

    @Override
    @Idempotent
    public BatchingVisitable<RowResult<byte[]>> getRange(
            TableReference tableRef,
            RangeRequest rangeRequest) {
        return this.<BatchingVisitable<RowResult<byte[]>>, BatchingVisitable<RowResult<byte[]>>> transformingDelegate(
                "getRange",
                () -> super.getRange(tableRef, rangeRequest),
                CachedBatchingVisitable::cache,
                s -> s,
                tableRef, rangeRequest);
    }

    @Override
    @Idempotent
    public Iterable<BatchingVisitable<RowResult<byte[]>>> getRanges(
            TableReference tableRef,
            Iterable<RangeRequest> rangeRequests) {
        return this.<Iterable<BatchingVisitable<RowResult<byte[]>>>, Iterable<BatchingVisitable<RowResult<byte[]>>>>transformingDelegate(
                "getRanges",
                () -> super.getRanges(tableRef, rangeRequests),
                result -> {
                    List<BatchingVisitable<RowResult<byte[]>>> serialized = Lists.newArrayList();
                    result.forEach(item -> serialized.add(CachedBatchingVisitable.cache(item)));
                    return serialized;
                },
                s -> s,
                tableRef, rangeRequests);
    }

    @Override
    @Idempotent
    public void commit() throws TransactionFailedException {
        TransactionFailedException failure = delegate("commit", () -> {
            try {
                super.commit();
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
        TransactionFailedException failure = delegate("commit", () -> {
            try {
                super.commit(transactionService);
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
        return delegate("getTimestamp", super::getTimestamp);
    }

    private <T> T delegate(String methodName, Supplier<T> getResult, Object... arguments) {
        return transformingDelegate(methodName, getResult, Function.identity(), Function.identity());
    }

    private <T, Y> T transformingDelegate(String methodName, Supplier<T> getResult, Function<T, Y> serializer, Function<Y, T> reverter, Object... arguments) {
        if (!Tracer.isTraceObservable()) {
            return getResult.get();
        }
        OpenSpan span = Tracer.startSpan(methodName, SpanType.CLIENT_OUTGOING);
        span.getRequestBuffer().ifPresent(buffer -> unsafeBufferWrite(buffer, arguments));
        Y result = serializer.apply(getResult.get());
        span.getResponseBuffer().ifPresent(buffer -> unsafeBufferWrite(buffer, result));
        Tracer.completeSpan();
        return reverter.apply(result);
    }

    private void unsafeBufferWrite(Buffer buffer, Object object) {
        try {
            new ObjectOutputStream(buffer.outputStream()).writeObject(object);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
}
