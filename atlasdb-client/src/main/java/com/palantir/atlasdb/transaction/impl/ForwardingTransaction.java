/**
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
package com.palantir.atlasdb.transaction.impl;

import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

import com.google.common.collect.ForwardingObject;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.transaction.api.ConstraintCheckable;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionFailedException;
import com.palantir.atlasdb.transaction.api.TransactionReadSentinelBehavior;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.common.base.BatchingVisitable;

public abstract class ForwardingTransaction extends ForwardingObject implements Transaction {

    @Override
    public void useTable(String tableName, ConstraintCheckable table) {
        delegate().useTable(tableName, table);
    }

    @Override
    public abstract Transaction delegate();

    @Override
    public SortedMap<byte[], RowResult<byte[]>> getRows(String tableName,
                                                        Iterable<byte[]> rows,
                                                        ColumnSelection columnSelection) {
        return delegate().getRows(tableName, rows, columnSelection);
    }

    @Override
    public Map<Cell, byte[]> get(String tableName, Set<Cell> cells) {
        return delegate().get(tableName, cells);
    }

    @Override
    public BatchingVisitable<RowResult<byte[]>> getRange(String tableName, RangeRequest rangeRequest) {
        return delegate().getRange(tableName, rangeRequest);
    }

    @Override
    public Iterable<BatchingVisitable<RowResult<byte[]>>> getRanges(String tableName,
                                                                    Iterable<RangeRequest> rangeRequests) {
        return delegate().getRanges(tableName, rangeRequests);
    }

    @Override
    public void put(String tableName, Map<Cell, byte[]> values) {
        delegate().put(tableName, values);
    }

    @Override
    public void delete(String tableName, Set<Cell> keys) {
        delegate().delete(tableName, keys);
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
