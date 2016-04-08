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

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
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
    public SortedMap<byte[], RowResult<byte[]>> getRows(String tableName,
                                                        Iterable<byte[]> rows,
                                                        ColumnSelection columnSelection) {
        checkTableName(tableName);
        return delegate().getRows(tableName, rows, columnSelection);
    }

    @Override
    public Map<Cell, byte[]> get(String tableName, Set<Cell> cells) {
        checkTableName(tableName);
        return delegate().get(tableName, cells);
    }

    @Override
    public BatchingVisitable<RowResult<byte[]>> getRange(String tableName, RangeRequest rangeRequest) {
        checkTableName(tableName);
        return delegate().getRange(tableName, rangeRequest);
    }

    @Override
    public Iterable<BatchingVisitable<RowResult<byte[]>>> getRanges(String tableName,
                                                                    Iterable<RangeRequest> rangeRequests) {
        checkTableName(tableName);
        return delegate().getRanges(tableName, rangeRequests);
    }

    private void checkTableName(String tableName) {
        SweepStrategy sweepStrategy = sweepStrategies.get().get(tableName);
        if (sweepStrategy == SweepStrategy.THOROUGH) {
            throw new IllegalStateException("Cannot read from a table with a thorough sweep strategy in a read only transaction.");
        }
    }

    @Override
    public void put(String tableName, Map<Cell, byte[]> values) {
        throw new IllegalArgumentException("This is a read only transaction.");
    }

    @Override
    public void delete(String tableName, Set<Cell> keys) {
        throw new IllegalArgumentException("This is a read only transaction.");
    }

}
