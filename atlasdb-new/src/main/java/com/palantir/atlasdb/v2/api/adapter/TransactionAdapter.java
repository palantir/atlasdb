/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.v2.api.adapter;

import static java.util.stream.Collectors.toSet;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.encoding.PtBytes;
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
import com.palantir.atlasdb.v2.api.api.GetOperations;
import com.palantir.atlasdb.v2.api.api.NewEndOperation;
import com.palantir.atlasdb.v2.api.api.NewIds;
import com.palantir.atlasdb.v2.api.api.NewIds.Row;
import com.palantir.atlasdb.v2.api.api.NewIds.Table;
import com.palantir.atlasdb.v2.api.api.NewPutOperation;
import com.palantir.atlasdb.v2.api.api.NewRowResult;
import com.palantir.atlasdb.v2.api.api.NewTransaction;
import com.palantir.atlasdb.v2.api.api.ScanFilter;
import com.palantir.common.base.BatchingVisitable;
import com.palantir.common.streams.KeyedStream;

public final class TransactionAdapter implements Transaction {
    private final NewTransaction txn;

    public TransactionAdapter(NewTransaction txn) {
        this.txn = txn;
    }

    @Override
    public SortedMap<byte[], RowResult<byte[]>> getRows(
            TableReference tableRef, Iterable<byte[]> rows, ColumnSelection columnSelection) {
        ListenableFuture<Map<Row, NewRowResult>> result = txn.get(GetOperations.getRows(
                toModern(tableRef),
                Streams.stream(rows).map(NewIds::row).collect(toSet()),
                toModern(columnSelection)));
        return Futures.getUnchecked(Futures.transform(result, results -> {
            ImmutableSortedMap.Builder<byte[], RowResult<byte[]>> builder =
                    ImmutableSortedMap.orderedBy(PtBytes.BYTES_COMPARATOR);
            for (Map.Entry<Row, NewRowResult> entry : results.entrySet()) {
                byte[] row = entry.getKey().toByteArray();
                builder.put(
                        row,
                        RowResult.create(row, KeyedStream.stream(entry.getValue().getColumns())
                                .mapKeys(NewIds.Column::toByteArray)
                                .map(NewIds.StoredValue::toByteArray)
                                .collectTo(() -> new TreeMap<>(PtBytes.BYTES_COMPARATOR))));
            }
            return builder.build();
        }, MoreExecutors.directExecutor()));
    }

    @Override
    public Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> getRowsColumnRangeIterator(TableReference tableRef,
            Iterable<byte[]> rows, BatchColumnRangeSelection columnRangeSelection) {
        return null;
    }

    @Override
    public BatchingVisitable<RowResult<byte[]>> getRange(TableReference tableRef, RangeRequest rangeRequest) {
        return null;
    }

    @Override
    public Map<Cell, byte[]> get(TableReference tableRef, Set<Cell> cells) {
        return Futures.getUnchecked(getAsync(tableRef, cells));
    }

    @Override
    public ListenableFuture<Map<Cell, byte[]>> getAsync(TableReference tableRef, Set<Cell> cells) {
        Set<NewIds.Cell> modern = cells.stream().map(TransactionAdapter::toModern).collect(toSet());
        return Futures.transform(txn.get(GetOperations.getCells(toModern(tableRef), modern)),
                result -> KeyedStream.stream(result)
                    .mapKeys(TransactionAdapter::toLegacy)
                    .map(NewIds.StoredValue::toByteArray)
                    .collectToMap(),
                MoreExecutors.directExecutor());
    }

    @Override
    public void put(TableReference tableRef, Map<Cell, byte[]> values) {
        Table table = toModern(tableRef);
        KeyedStream.stream(values)
                .mapKeys(TransactionAdapter::toModern)
                .map(NewIds::value)
                .map(Optional::of)
                .map((cell, value) -> NewPutOperation.of(table, cell, value))
                .values()
                .forEach(txn::put);
    }

    @Override
    public void delete(TableReference tableRef, Set<Cell> keys) {
        Table table = toModern(tableRef);
        keys.stream()
                .map(TransactionAdapter::toModern)
                .forEach(cell -> txn.put(NewPutOperation.of(table, cell, Optional.empty())));
    }

    @Override
    public TransactionType getTransactionType() {
        return TransactionType.DEFAULT;
    }

    @Override
    public void abort() {
        txn.end(NewEndOperation.ABORT);
    }

    @Override
    public void commit() throws TransactionFailedException {
        txn.end(NewEndOperation.COMMIT);
    }

    @Override
    public long getTimestamp() {
        return txn.startTimestamp();
    }

    @Override
    public TransactionReadSentinelBehavior getReadSentinelBehavior() {
        return TransactionReadSentinelBehavior.THROW_EXCEPTION;
    }

    @Override
    public void useTable(TableReference tableRef, ConstraintCheckable table) {}

    private static Table toModern(TableReference table) {
        return NewIds.table(table.toString());
    }

    private static NewIds.Cell toModern(Cell cell) {
        return NewIds.cell(NewIds.row(cell.getRowName()), NewIds.column(cell.getColumnName()));
    }

    private static Cell toLegacy(NewIds.Cell cell) {
        return Cell.create(cell.row().toByteArray(), cell.column().toByteArray());
    }

    private static ScanFilter.ColumnsFilter toModern(ColumnSelection selection) {
        if (selection.allColumnsSelected()) {
            return ScanFilter.allColumns();
        }
        return ScanFilter.exactColumns(selection.getSelectedColumns().stream().map(NewIds::column).collect(toSet()));
    }

    @Override
    public Iterator<Map.Entry<Cell, byte[]>> getRowsColumnRange(TableReference tableRef, Iterable<byte[]> rows,
            ColumnRangeSelection columnRangeSelection, int batchHint) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterable<BatchingVisitable<RowResult<byte[]>>> getRanges(
            TableReference tableRef, Iterable<RangeRequest> rangeRequests) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Stream<T> getRanges(TableReference tableRef, Iterable<RangeRequest> rangeRequests,
            int concurrencyLevel,
            BiFunction<RangeRequest, BatchingVisitable<RowResult<byte[]>>, T> visitableProcessor) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Stream<T> getRanges(TableReference tableRef, Iterable<RangeRequest> rangeRequests,
            BiFunction<RangeRequest, BatchingVisitable<RowResult<byte[]>>, T> visitableProcessor) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Stream<BatchingVisitable<RowResult<byte[]>>> getRangesLazy(
            TableReference tableRef, Iterable<RangeRequest> rangeRequests) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void disableReadWriteConflictChecking(TableReference tableRef) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void commit(TransactionService transactionService) throws TransactionFailedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isAborted() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isUncommitted() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTransactionType(TransactionType transactionType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> getRowsColumnRange(TableReference tableRef,
            Iterable<byte[]> rows, BatchColumnRangeSelection columnRangeSelection) {
        throw new UnsupportedOperationException();
    }
}
