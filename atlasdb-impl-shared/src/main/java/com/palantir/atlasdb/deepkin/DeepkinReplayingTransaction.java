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

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

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

public class DeepkinReplayingTransaction implements Transaction {
    private final ReplayerService replayerService;
    private final Transaction delegate;

    public DeepkinReplayingTransaction(ReplayerService replayerService, Transaction delegate) {
        this.replayerService = replayerService;
        this.delegate = delegate;
    }

    @Override
    @Idempotent
    public SortedMap<byte[], RowResult<byte[]>> getRows(
            TableReference tableRef, Iterable<byte[]> rows,
            ColumnSelection columnSelection) {
        return replayerService.getResult(TransactionMethod.GET_ROWS, tableRef, rows, columnSelection);
    }

    @Override
    @Idempotent
    public Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> getRowsColumnRange(
            TableReference tableRef, Iterable<byte[]> rows,
            BatchColumnRangeSelection columnRangeSelection) {
        return replayerService.getResult(TransactionMethod.GET_ROWS_COLUMN_RANGE, tableRef, rows, columnRangeSelection);
    }

    @Override
    @Idempotent
    public Iterator<Map.Entry<Cell, byte[]>> getRowsColumnRange(
            TableReference tableRef, Iterable<byte[]> rows,
            ColumnRangeSelection columnRangeSelection, int batchHint) {
        return replayerService.getResult(TransactionMethod.GET_BATCHED_ROWS_COLUMN_RANGE, tableRef, rows, columnRangeSelection, batchHint);
    }

    @Override
    @Idempotent
    public Map<Cell, byte[]> get(
            TableReference tableRef,
            Set<Cell> cells) {
        return replayerService.getResult(TransactionMethod.GET, tableRef, cells);
    }

    @Override
    @Idempotent
    public BatchingVisitable<RowResult<byte[]>> getRange(
            TableReference tableRef,
            RangeRequest rangeRequest) {
        return replayerService.getResult(TransactionMethod.GET_RANGE, tableRef, rangeRequest);
    }

    @Override
    @Idempotent
    public Iterable<BatchingVisitable<RowResult<byte[]>>> getRanges(
            TableReference tableRef,
            Iterable<RangeRequest> rangeRequests) {
        return replayerService.getResult(TransactionMethod.GET_RANGES, tableRef, rangeRequests);
    }

    @Override
    @Idempotent
    public void commit() throws TransactionFailedException {
        TransactionFailedException failure = replayerService.getResult(TransactionMethod.COMMIT);
        if (failure != null) {
            throw failure;
        }
    }

    @Override
    @Idempotent
    public void commit(TransactionService transactionService) throws TransactionFailedException {
        TransactionFailedException failure = replayerService.getResult(TransactionMethod.COMMIT_SERVICE);
        if (failure != null) {
            throw failure;
        }
    }

    @Override
    @Idempotent
    public long getTimestamp() {
        return replayerService.getResult(TransactionMethod.GET_TIMESTAMP);
    }

    @Override
    public void put(TableReference tableRef, Map<Cell, byte[]> values) {
        // no-op
    }

    @Override
    public void delete(TableReference tableRef, Set<Cell> keys) {
        // no-op
    }

    @Override
    public TransactionType getTransactionType() {
        return delegate.getTransactionType();
    }

    @Override
    public void setTransactionType(TransactionType transactionType) {
        delegate.setTransactionType(transactionType);
    }

    @Override
    public void abort() {
        // no-op
    }

    @Override
    public boolean isAborted() {
        return delegate.isAborted();
    }

    @Override
    public boolean isUncommitted() {
        return delegate.isUncommitted();
    }

    @Override
    public TransactionReadSentinelBehavior getReadSentinelBehavior() {
        return delegate.getReadSentinelBehavior();
    }

    @Override
    public void useTable(TableReference tableRef, ConstraintCheckable table) {
        delegate.useTable(tableRef, table);
    }
}
