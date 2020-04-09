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

package com.palantir.atlasdb.v2.api.kvs;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.cache.TimestampCache;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.sweep.queue.MultiTableSweepQueueWriter;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.v2.api.api.NewIds;
import com.palantir.atlasdb.v2.api.api.NewIds.Cell;
import com.palantir.atlasdb.v2.api.api.NewIds.Column;
import com.palantir.atlasdb.v2.api.api.NewIds.Row;
import com.palantir.atlasdb.v2.api.api.NewIds.Table;
import com.palantir.atlasdb.v2.api.api.NewValue;
import com.palantir.atlasdb.v2.api.api.NewValue.KvsValue;
import com.palantir.atlasdb.v2.api.api.NewValue.TransactionValue;
import com.palantir.atlasdb.v2.api.api.ScanDefinition;
import com.palantir.atlasdb.v2.api.api.ScanFilter;
import com.palantir.atlasdb.v2.api.api.Kvs;
import com.palantir.atlasdb.v2.api.api.AsyncIterator;
import com.palantir.atlasdb.v2.api.iterators.AsyncIterators;
import com.palantir.atlasdb.v2.api.iterators.IteratorFutureIterator;
import com.palantir.atlasdb.v2.api.transaction.state.TableWrites;
import com.palantir.atlasdb.v2.api.transaction.state.TransactionState;
import com.palantir.common.streams.KeyedStream;

import io.vavr.Tuple2;

public final class LegacyKvs implements Kvs {
    private static final int BATCH_HINT_FOR_ITERATORS = 1000;
    private final Executor executor;
    private final TransactionService transactionService;
    private final KeyValueService keyValueService;
    private final MultiTableSweepQueueWriter sweepQueue;
    private final TimestampCache timestampCache;
    private final AsyncIterators iterators;

    public LegacyKvs(Executor executor,
            TransactionService transactionService,
            KeyValueService keyValueService,
            MultiTableSweepQueueWriter sweepQueue,
            TimestampCache timestampCache) {
        this.executor = executor;
        this.transactionService = transactionService;
        this.keyValueService = keyValueService;
        this.sweepQueue = sweepQueue;
        this.timestampCache = timestampCache;
        this.iterators = new AsyncIterators(executor);
    }

    private ListenableFuture<?> run(Runnable runnable) {
        return call(() -> {
            runnable.run();
            return null;
        });
    }

    private <T> ListenableFuture<T> call(Callable<T> callable) {
        return Futures.submitAsync(() -> Futures.immediateFuture(callable.call()), executor);
    }

    @Override
    public ListenableFuture<Map<Cell, KvsValue>> loadCellsAtTimestamps(Table table, Map<Cell, Long> timestampsToLoadAt) {
        return call(() -> {
            Map<com.palantir.atlasdb.keyvalue.api.Cell, Long> legacy = KeyedStream.stream(timestampsToLoadAt)
                    .mapKeys(LegacyKvs::toLegacy)
                    .collectToMap();
            return KeyedStream.stream(keyValueService.get(toLegacy(table), legacy))
                    .mapKeys(cell -> fromLegacy(cell))
                    .map((cell, value) -> fromLegacy(cell, value))
                    .collectToMap();
        });
    }

    @Override
    public ListenableFuture<Map<Long, Long>> getCommitTimestamps(Set<Long> timestamps) {
        return call(() -> {
            Map<Long, Long> results = transactionService.get(timestamps);
            results.forEach(timestampCache::putAlreadyCommittedTransaction);
            return results;
        });
    }

    @Override
    public OptionalLong getCachedCommitTimestamp(long startTimestamp) {
        Long maybeTimestamp = timestampCache.getCommitTimestampIfPresent(startTimestamp);
        if (maybeTimestamp == null) {
            return OptionalLong.empty();
        } else {
            return OptionalLong.of(maybeTimestamp);
        }
    }

    @Override
    public ListenableFuture<?> write(TransactionState state) {
        return run(() -> {
            Map<TableReference, Map<com.palantir.atlasdb.keyvalue.api.Cell, byte[]>> writes =
                    KeyedStream.of(state.writes())
                            .mapKeys(TableWrites::table)
                            .mapKeys(LegacyKvs::toLegacy)
                            .map(TableWrites::data)
                            .map(data -> KeyedStream.of(data)
                                    .mapKeys(Tuple2::_1)
                                    .mapKeys(LegacyKvs::toLegacy)
                                    .map(Tuple2::_2)
                                    .map(LegacyKvs::toLegacy)
                                    .collectToMap())
                            .collectToMap();
            // this actually doesn't guarantee what we wanted to which is that everything should get cleaned up, eventually.
            // to do that, we'd have to shove in a cheeky lock check.
            sweepQueue.enqueue(writes, state.startTimestamp());
            keyValueService.multiPut(writes, state.startTimestamp());
        });
    }

    @Override
    public ListenableFuture<?> commit(TransactionState state) {
        return run(() -> {
            long start = state.startTimestamp();
            long commit = state.commitTimestamp().getAsLong();
            try {
                transactionService.putUnlessExists(start, commit);
            } catch (KeyAlreadyExistsException e) {
                if (transactionService.get(start).equals(commit)) {
                    return;
                } else {
                    throw e;
                }
            }
        });
    }

    @Override
    public AsyncIterator<KvsValue> scan(TransactionState state, ScanDefinition definition) {
        return definition.filter().rows().accept(new ScanFilter.RowsFilter.Visitor<AsyncIterator<KvsValue>>() {
            @Override
            public AsyncIterator<KvsValue> visitAllRows() {
                return visitRowRange(Optional.empty(), Optional.empty());
            }

            @Override
            public AsyncIterator<KvsValue> visitExactRows(Set<Row> rows) {
                return definition.filter().columns().accept(
                        new ScanFilter.ColumnsFilter.Visitor<AsyncIterator<KvsValue>>() {
                            @Override
                            public AsyncIterator<KvsValue> visitAllColumns() {
                                return execute(ColumnSelection.all());
                            }

                            @Override
                            public AsyncIterator<KvsValue> visitExactColumns(Set<Column> columns) {
                                return execute(toColumnSelection(definition.filter().columns()));
                            }

                            private AsyncIterator<KvsValue> execute(ColumnSelection columnSelection) {
                                return new IteratorFutureIterator<>(call(() -> {
                                    Iterable<byte[]> byteArrayRows = Iterables.transform(rows, Row::toByteArray);
                                    Map<com.palantir.atlasdb.keyvalue.api.Cell, Value> rows = keyValueService.getRows(
                                            toLegacy(definition.table()),
                                            byteArrayRows,
                                            columnSelection,
                                            state.startTimestamp());
                                    return KeyedStream.stream(rows)
                                            .mapKeys(cell -> fromLegacy(cell))
                                            .map((cell, value) -> fromLegacy(cell, value))
                                            .values()
                                            .sorted(definition.attributes().cellComparator())
                                            .iterator();
                                }));
                            }

                            @Override
                            public AsyncIterator<KvsValue> visitColumnRange(
                                    Optional<Column> fromInclusive, Optional<Column> toExclusive) {
                                throw new UnsupportedOperationException();
                            }
                        });
            }

            @Override
            public AsyncIterator<KvsValue> visitRowRange(Optional<Row> fromInclusive,
                    Optional<Row> toExclusive) {
                ColumnSelection columnSelection = toColumnSelection(definition.filter().columns());
                RangeRequest request = range(fromInclusive, toExclusive, columnSelection);
                Iterator<RowResult<Value>> rows = keyValueService.getRange(
                        toLegacy(definition.table()),
                        request,
                        state.startTimestamp());
                return iterators.concat(iterators.transform(toAsyncIterator(rows), rowResult -> {
                    List<KvsValue> results = new ArrayList<>(rowResult.getColumns().size());
                    Row row = NewIds.row(rowResult.getRowName());
                    rowResult.getColumns().forEach((column, value) -> {
                        Column c = NewIds.column(column);
                        Cell cell = NewIds.cell(row, c);
                        results.add(fromLegacy(cell, value));
                    });
                    return results.iterator();
                }));
            }
        });
    }

    // this... might work.
    private <T> AsyncIterator<T> toAsyncIterator(Iterator<T> iterator) {
        Iterator<List<T>> pageIterator = Iterators.partition(iterator, BATCH_HINT_FOR_ITERATORS);
        AsyncIterator<Iterator<T>> asyncPageIterator = new AsyncIterator<Iterator<T>>() {
            @Override
            public ListenableFuture<Boolean> onHasNext() {
                return call(pageIterator::hasNext);
            }

            @Override
            public boolean hasNext() {
                return pageIterator.hasNext();
            }

            @Override
            public Iterator<T> next() {
                return pageIterator.next().iterator();
            }
        };
        return iterators.concat(asyncPageIterator);
    }

    private static ColumnSelection toColumnSelection(ScanFilter.ColumnsFilter filter) {
        return filter.accept(new ScanFilter.ColumnsFilter.Visitor<ColumnSelection>() {
            @Override
            public ColumnSelection visitAllColumns() {
                return ColumnSelection.all();
            }

            @Override
            public ColumnSelection visitExactColumns(Set<Column> columns) {
                return ColumnSelection.create(Iterables.transform(columns, Column::toByteArray));
            }

            // todo postfilter this properly
            @Override
            public ColumnSelection visitColumnRange(Optional<Column> fromInclusive,
                    Optional<Column> toExclusive) {
                return ColumnSelection.all();
            }
        });
    }

    private static RangeRequest range(
            Optional<Row> fromInclusive, Optional<Row> toExclusive, ColumnSelection selection) {
        RangeRequest.Builder builder = RangeRequest.builder();
        fromInclusive.map(Row::toByteArray).ifPresent(builder::startRowInclusive);
        toExclusive.map(Row::toByteArray).ifPresent(builder::endRowExclusive);
        builder.retainColumns(selection);
        // todo batch hint
        return builder.build();
    }

    @SuppressWarnings("deprecation")
    private static TableReference toLegacy(Table table) {
        return TableReference.createUnsafe(table.getName());
    }

    private static KvsValue fromLegacy(Cell cell, Value value) {
        return NewValue.kvsValue(cell, value.getTimestamp(), create(value.getContents()));
    }

    private static Optional<NewIds.StoredValue> create(byte[] legacyArray) {
        if (legacyArray.length == 0) {
            return Optional.empty();
        }
        return Optional.of(NewIds.value(legacyArray));
    }

    private static byte[] toLegacy(TransactionValue value) {
        return value.maybeData().map(NewIds.StoredValue::toByteArray).orElse(PtBytes.EMPTY_BYTE_ARRAY);
    }

    private static com.palantir.atlasdb.keyvalue.api.Cell toLegacy(Cell cell) {
        return com.palantir.atlasdb.keyvalue.api.Cell.create(cell.row().toByteArray(), cell.column().toByteArray());
    }

    private static Cell fromLegacy(com.palantir.atlasdb.keyvalue.api.Cell cell) {
        return NewIds.cell(NewIds.row(cell.getRowName()), NewIds.column(cell.getColumnName()));
    }
}
