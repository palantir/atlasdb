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

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

import com.palantir.atlasdb.cache.TimestampCache;
import com.palantir.atlasdb.cleaner.Cleaner;
import com.palantir.atlasdb.deepkin.DeepkinTransaction;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.ConstraintCheckable;
import com.palantir.atlasdb.transaction.api.TransactionFailedException;
import com.palantir.atlasdb.transaction.api.TransactionReadSentinelBehavior;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.common.annotation.Idempotent;
import com.palantir.common.base.BatchingVisitable;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.RemoteLockService;
import com.palantir.timestamp.TimestampService;

public class SerializableDeepkinTransaction extends SerializableTransaction {
    private final DeepkinTransaction delegate;

    public SerializableDeepkinTransaction(KeyValueService keyValueService, RemoteLockService remoteLockService,
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
        this.delegate = new DeepkinTransaction(this);
    }

    @Override
    @Idempotent
    public SortedMap<byte[], RowResult<byte[]>> getRows(
            TableReference tableRef, Iterable<byte[]> rows,
            ColumnSelection columnSelection) {
        return delegate.getRows(tableRef, rows, columnSelection);
    }

    @Override
    @Idempotent
    public Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> getRowsColumnRange(
            TableReference tableRef, Iterable<byte[]> rows,
            BatchColumnRangeSelection columnRangeSelection) {
        return delegate.getRowsColumnRange(tableRef, rows, columnRangeSelection);
    }

    @Override
    @Idempotent
    public Iterator<Map.Entry<Cell, byte[]>> getRowsColumnRange(
            TableReference tableRef, Iterable<byte[]> rows,
            ColumnRangeSelection columnRangeSelection, int batchHint) {
        return delegate.getRowsColumnRange(tableRef, rows, columnRangeSelection, batchHint);
    }

    @Override
    @Idempotent
    public Map<Cell, byte[]> get(
            TableReference tableRef,
            Set<Cell> cells) {
        return delegate.get(tableRef, cells);
    }

    @Override
    @Idempotent
    public BatchingVisitable<RowResult<byte[]>> getRange(
            TableReference tableRef,
            RangeRequest rangeRequest) {
        return delegate.getRange(tableRef, rangeRequest);
    }

    @Override
    @Idempotent
    public Iterable<BatchingVisitable<RowResult<byte[]>>> getRanges(
            TableReference tableRef,
            Iterable<RangeRequest> rangeRequests) {
        return delegate.getRanges(tableRef, rangeRequests);
    }

    @Override
    @Idempotent
    public void commit() throws TransactionFailedException {
        delegate.commit();
    }

    @Override
    @Idempotent
    public void commit(TransactionService transactionService) throws TransactionFailedException {
        delegate.commit(transactionService);
    }

    @Override
    @Idempotent
    public long getTimestamp() {
        return delegate.getTimestamp();
    }

    @Override
    @Idempotent
    public void put(TableReference tableRef,
            Map<Cell, byte[]> values) {
        delegate.put(tableRef, values);
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
