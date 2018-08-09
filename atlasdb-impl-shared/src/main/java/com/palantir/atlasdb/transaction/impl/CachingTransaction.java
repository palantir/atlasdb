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

import static java.util.Collections.emptyMap;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import com.google.common.collect.Sets;
import com.google.common.primitives.UnsignedBytes;
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

public class CachingTransaction implements Transaction {
    private final Map<TableReference, Map<Cell, byte[]>> cache;
    private final SerializableTransaction delegate;
    private final Set<Cell> cacheMisses = Sets.newConcurrentHashSet();

    public CachingTransaction(SerializableTransaction delegate) {
        this.cache = emptyMap();
        this.delegate = delegate;
    }

    private Map<Cell, byte[]> getCache(TableReference ref) {
        return cache.computeIfAbsent(ref, k -> new HashMap<>());
    }

    @Override
    public SortedMap<byte[], RowResult<byte[]>> getRows(
            TableReference tableRef, Iterable<byte[]> rows, ColumnSelection columnSelection) {
        if (columnSelection.allColumnsSelected()) {
            return delegate.getRows(tableRef, rows, columnSelection);
        }
        Map<Cell, byte[]> cache = getCache(tableRef);
        Map<Cell, byte[]> cached = new HashMap<>();
        Set<Cell> toQuery = new HashSet<>();
        for (byte[] row : rows) {
            for (byte[] column : columnSelection.getSelectedColumns()) {
                Cell cell = Cell.create(row, column);
                byte[] value = cache.get(cell);
                if (value == null) {
                    toQuery.add(Cell.create(row, column));
                } else {
                    cached.put(cell, value);
                }
            }
        }
        delegate.addToReadSet(tableRef, cached);
        Map<Cell, byte[]> fetched = delegate.get(tableRef, toQuery);
        SortedMap<byte[], RowResult<byte[]>> result = new TreeMap<>(UnsignedBytes.lexicographicalComparator());
        rows.forEach(row -> {
            SortedMap<byte[], byte[]> values = new TreeMap<>(UnsignedBytes.lexicographicalComparator());
            columnSelection.getSelectedColumns().forEach(column -> {
                Cell cell = Cell.create(row, column);
                byte[] cachedValue = cached.get(cell);
                if (cachedValue != null) {
                    values.put(column, cachedValue);
                    return;
                }
                byte[] fetchedValue = fetched.get(cell);
                if (fetchedValue != null) {
                    values.put(column, fetchedValue);
                }
            });
            result.put(row, RowResult.create(row, values));
        });
        addCacheMisses(tableRef, fetched);
        return result;
    }

    @Override
    public Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> getRowsColumnRange(TableReference tableRef,
            Iterable<byte[]> rows, BatchColumnRangeSelection columnRangeSelection) {
        return delegate.getRowsColumnRange(tableRef, rows, columnRangeSelection);
    }

    @Override
    public Iterator<Map.Entry<Cell, byte[]>> getRowsColumnRange(TableReference tableRef, Iterable<byte[]> rows,
            ColumnRangeSelection columnRangeSelection, int batchHint) {
        return delegate.getRowsColumnRange(tableRef, rows, columnRangeSelection, batchHint);
    }

    @Override
    public Map<Cell, byte[]> get(TableReference tableRef, Set<Cell> cells) {
        Map<Cell, byte[]> results = new HashMap<>();
        Set<Cell> toQuery = new HashSet<>();

        Map<Cell, byte[]> cache = getCache(tableRef);
        for (Cell cell : cells) {
            byte[] data = cache.get(cell);
            if (data == null) {
                toQuery.add(cell);
            } else {
                results.put(cell, data);
            }
        }
        Map<Cell, byte[]> fetched = delegate.get(tableRef, toQuery);
        addCacheMisses(tableRef, fetched);
        results.putAll(delegate.get(tableRef, toQuery));
        return results;
    }

    private synchronized void addCacheMisses(TableReference reference, Map<Cell, byte[]> misses) {
        getCache(reference).putAll(misses);
        cacheMisses.addAll(misses.keySet());
    }

    @Override
    public BatchingVisitable<RowResult<byte[]>> getRange(TableReference tableRef, RangeRequest rangeRequest) {
        return delegate.getRange(tableRef, rangeRequest);
    }

    @Override
    public Iterable<BatchingVisitable<RowResult<byte[]>>> getRanges(
            TableReference tableRef, Iterable<RangeRequest> rangeRequests) {
        return delegate.getRanges(tableRef, rangeRequests);
    }

    @Override
    public <T> Stream<T> getRanges(
            TableReference tableRef, Iterable<RangeRequest> rangeRequests, int concurrencyLevel,
            BiFunction<RangeRequest, BatchingVisitable<RowResult<byte[]>>, T> visitableProcessor) {
        return delegate.getRanges(tableRef, rangeRequests, concurrencyLevel, visitableProcessor);
    }

    @Override
    public <T> Stream<T> getRanges(TableReference tableRef, Iterable<RangeRequest> rangeRequests,
            BiFunction<RangeRequest, BatchingVisitable<RowResult<byte[]>>, T> visitableProcessor) {
        return getRanges(tableRef, rangeRequests, visitableProcessor);
    }

    @Override
    public Stream<BatchingVisitable<RowResult<byte[]>>> getRangesLazy(TableReference tableRef,
            Iterable<RangeRequest> rangeRequests) {
        return getRangesLazy(tableRef, rangeRequests);
    }

    @Override
    public synchronized void put(TableReference tableRef, Map<Cell, byte[]> values) {
        Map<Cell, byte[]> cache = getCache(tableRef);
        cache.putAll(values);
        delegate.put(tableRef, values);
    }

    @Override
    public synchronized void delete(TableReference tableRef, Set<Cell> keys) {
        Map<Cell, byte[]> cache = getCache(tableRef);
        keys.forEach(cache::remove);
        delegate.delete(tableRef, keys);
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
        delegate.abort();
    }

    @Override
    public void commit() throws TransactionFailedException {
        delegate.commit();
    }

    @Override
    public void commit(TransactionService transactionService) throws TransactionFailedException {
        delegate.commit(transactionService);
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
    public long getTimestamp() {
        return delegate.getTimestamp();
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
