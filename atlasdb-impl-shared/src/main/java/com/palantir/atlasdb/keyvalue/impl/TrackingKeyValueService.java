/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.impl;

import static com.palantir.atlasdb.keyvalue.impl.Cells.getApproxSizeOfCell;

import com.google.common.collect.ForwardingIterator;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.streams.KeyedStream;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class TrackingKeyValueService extends ForwardingKeyValueService {
    private final KeyValueService delegate;
    private final AtomicLong bytesRead = new AtomicLong(0);
    private final AtomicLong readCallsMade = new AtomicLong(0);
    private final AtomicLong maxBytesReadInOneReadCall = new AtomicLong(0);
    private final Set<TableReference> tablesReadFrom = Sets.newConcurrentHashSet();

    public TrackingKeyValueService(KeyValueService delegate, MetricsManager metricsManager) {
        this.delegate = delegate;
    }

    @Override
    public KeyValueService delegate() {
        return delegate;
    }

    @Override
    public ListenableFuture<Map<Cell, Value>> getAsync(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        tablesReadFrom.add(tableRef);
        readCallsMade.incrementAndGet();
        return Futures.transform(
                delegate.getAsync(tableRef, timestampByCell),
                cellToValue -> {
                    long mapBytes = getApproximateMapBytes(cellToValue);
                    bytesRead.addAndGet(mapBytes);
                    maxBytesReadInOneReadCall.accumulateAndGet(mapBytes, Long::max);

                    return cellToValue;
                },
                MoreExecutors.directExecutor());
    }

    @Override
    public Map<Cell, Value> getRows(
            TableReference tableRef, Iterable<byte[]> rows, ColumnSelection columnSelection, long timestamp) {
        tablesReadFrom.add(tableRef);
        readCallsMade.incrementAndGet();

        Map<Cell, Value> cellToValue = delegate.getRows(tableRef, rows, columnSelection, timestamp);

        long mapBytes = getApproximateMapBytes(cellToValue);
        bytesRead.addAndGet(mapBytes);
        maxBytesReadInOneReadCall.accumulateAndGet(mapBytes, Long::max);

        return cellToValue;
    }

    @Override
    public Map<byte[], RowColumnRangeIterator> getRowsColumnRange(
            TableReference tableRef,
            Iterable<byte[]> rows,
            BatchColumnRangeSelection batchColumnRangeSelection,
            long timestamp) {
        tablesReadFrom.add(tableRef);
        Map<byte[], RowColumnRangeIterator> results =
                delegate.getRowsColumnRange(tableRef, rows, batchColumnRangeSelection, timestamp);
        results.replaceAll((unused, iterator) -> new ForwardingRowColumnRangeIterator(iterator));
        return results;
    }

    private long getApproximateMapBytes(Map<Cell, Value> cellToValue) {
        return KeyedStream.stream(cellToValue)
                .map((cell, value) -> getApproxSizeOfCell(cell) + value.getByteCount())
                .values()
                .mapToLong(i -> i)
                .sum();
    }

    class ForwardingRowColumnRangeIterator extends ForwardingIterator<Map.Entry<Cell, Value>>
            implements RowColumnRangeIterator {
        // keep tally and max that as we go
        Iterator<Map.Entry<Cell, Value>> delegate;

        private ForwardingRowColumnRangeIterator(Iterator<Map.Entry<Cell, Value>> delegate) {
            this.delegate = delegate;
        }

        @Override
        protected Iterator<Map.Entry<Cell, Value>> delegate() {
            return delegate;
        }

        @Override
        public Map.Entry<Cell, Value> next() {
            Map.Entry<Cell, Value> entry = delegate().next();
            bytesRead.addAndGet(
                    getApproxSizeOfCell(entry.getKey()) + entry.getValue().getByteCount());
            return entry;
        }
    }
}
