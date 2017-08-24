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
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ExecutorService;

import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.common.base.BatchingVisitable;

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
    public SortedMap<byte[], RowResult<byte[]>> getRows(TableReference tableRef,
                                                        Iterable<byte[]> rows,
                                                        ColumnSelection columnSelection) {
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
    public Iterable<BatchingVisitable<RowResult<byte[]>>> getRanges(TableReference tableRef,
                                                                    Iterable<RangeRequest> rangeRequests) {
        checkTableName(tableRef);
        return delegate().getRanges(tableRef, rangeRequests);
    }

    @Override
    public Iterable<BatchingVisitable<RowResult<byte[]>>> getUnfetchedRanges(
            final TableReference tableRef, Iterable<RangeRequest> rangeRequests) {
        checkTableName(tableRef);
        return delegate().getUnfetchedRanges(tableRef, rangeRequests);
    }

    @Override
    public Iterable<BatchingVisitable<RowResult<byte[]>>> getRangesWithFirstPages(
            final TableReference tableRef,
            Iterable<RangeRequest> rangeRequests,
            int prefetchConcurrency,
            ExecutorService executorService) {
        checkTableName(tableRef);
        return delegate().getRangesWithFirstPages(tableRef, rangeRequests, prefetchConcurrency, executorService);
    }

    @Override
    public Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> getRowsColumnRange(TableReference tableRef,
            Iterable<byte[]> rows, BatchColumnRangeSelection columnRangeSelection) {
        checkTableName(tableRef);
        return delegate().getRowsColumnRange(tableRef, rows, columnRangeSelection);
    }

    @Override
    public Iterator<Map.Entry<Cell, byte[]>> getRowsColumnRange(TableReference tableRef, Iterable<byte[]> rows,
            ColumnRangeSelection columnRangeSelection, int batchHint) {
        checkTableName(tableRef);
        return delegate().getRowsColumnRange(tableRef, rows, columnRangeSelection, batchHint);
    }

    private void checkTableName(TableReference tableRef) {
        SweepStrategy sweepStrategy = sweepStrategies.get().get(tableRef);
        if (sweepStrategy == SweepStrategy.THOROUGH) {
            throw new IllegalStateException("Cannot read from a table with a thorough sweep strategy in a read only transaction.");
        }
    }

    @Override
    public void put(TableReference tableRef, Map<Cell, byte[]> values) {
        throw new IllegalArgumentException("This is a read only transaction.");
    }

    @Override
    public void delete(TableReference tableRef, Set<Cell> keys) {
        throw new IllegalArgumentException("This is a read only transaction.");
    }

}
