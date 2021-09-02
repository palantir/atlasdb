/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.api.cache;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.primitives.UnsignedBytes;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.TransactionLockWatchFailedException;
import com.palantir.atlasdb.util.ByteArrayUtilities;
import com.palantir.common.streams.KeyedStream;
import com.palantir.lock.watch.CommitUpdate;
import com.palantir.logsafe.Arg;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Arrays;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

final class ValidatingTransactionScopedCache implements TransactionScopedCache {
    private static final SafeLogger log = SafeLoggerFactory.get(ValidatingTransactionScopedCache.class);

    private final TransactionScopedCache delegate;
    private final Random random;
    private final double validationProbability;
    private final Runnable failureCallback;

    @VisibleForTesting
    ValidatingTransactionScopedCache(
            TransactionScopedCache delegate, double validationProbability, Runnable failureCallback) {
        this.delegate = delegate;
        this.validationProbability = validationProbability;
        this.failureCallback = failureCallback;
        this.random = new Random();
    }

    static TransactionScopedCache create(
            TransactionScopedCache delegate, double validationProbability, Runnable failureCallback) {
        return new ValidatingTransactionScopedCache(delegate, validationProbability, failureCallback);
    }

    @Override
    public void write(TableReference tableReference, Map<Cell, byte[]> values) {
        delegate.write(tableReference, values);
    }

    @Override
    public void delete(TableReference tableReference, Set<Cell> cells) {
        delegate.delete(tableReference, cells);
    }

    @Override
    public Map<Cell, byte[]> get(
            TableReference tableReference,
            Set<Cell> cells,
            BiFunction<TableReference, Set<Cell>, ListenableFuture<Map<Cell, byte[]>>> valueLoader) {
        return AtlasFutures.getUnchecked(getAsync(tableReference, cells, valueLoader));
    }

    @Override
    public ListenableFuture<Map<Cell, byte[]>> getAsync(
            TableReference tableReference,
            Set<Cell> cells,
            BiFunction<TableReference, Set<Cell>, ListenableFuture<Map<Cell, byte[]>>> valueLoader) {
        if (shouldValidate()) {
            ListenableFuture<Map<Cell, byte[]>> remoteReads = valueLoader.apply(tableReference, cells);
            ListenableFuture<Map<Cell, byte[]>> cacheReads = delegate.getAsync(
                    tableReference,
                    cells,
                    (table, cellsToRead) -> Futures.transform(
                            remoteReads, reads -> getCells(reads, cellsToRead), MoreExecutors.directExecutor()));

            return Futures.transform(
                    cacheReads,
                    reads -> {
                        validateCacheReads(tableReference, AtlasFutures.getDone(remoteReads), reads);
                        return reads;
                    },
                    MoreExecutors.directExecutor());
        } else {
            return delegate.getAsync(tableReference, cells, valueLoader);
        }
    }

    @Override
    public NavigableMap<byte[], RowResult<byte[]>> getRows(
            TableReference tableRef,
            Iterable<byte[]> rows,
            ColumnSelection columnSelection,
            Function<Set<Cell>, Map<Cell, byte[]>> cellLoader,
            Function<Iterable<byte[]>, NavigableMap<byte[], RowResult<byte[]>>> rowLoader) {
        if (shouldValidate()) {
            NavigableMap<byte[], RowResult<byte[]>> remoteReads = rowLoader.apply(rows);
            NavigableMap<byte[], RowResult<byte[]>> cacheReads = delegate.getRows(
                    tableRef,
                    rows,
                    columnSelection,
                    cellsToRead -> KeyedStream.of(cellsToRead)
                            .map(cell -> Optional.ofNullable(remoteReads.get(cell.getRowName()))
                                    .map(RowResult::getColumns)
                                    .map(col -> col.get(cell.getColumnName()))
                                    .orElse(null))
                            .filter(Objects::nonNull)
                            .collectToMap(),
                    rowsToRead -> {
                        SortedSet<byte[]> toReadSorted = new TreeSet<>(UnsignedBytes.lexicographicalComparator());
                        rowsToRead.forEach(toReadSorted::add);
                        return Maps.filterKeys(remoteReads, toReadSorted::contains);
                    });
            validateCacheRowReads(tableRef, remoteReads, cacheReads);
            return cacheReads;
        } else {
            return delegate.getRows(tableRef, rows, columnSelection, cellLoader, rowLoader);
        }
    }

    @Override
    public void finalise() {
        delegate.finalise();
    }

    @Override
    public ValueDigest getValueDigest() {
        return delegate.getValueDigest();
    }

    @Override
    public HitDigest getHitDigest() {
        return delegate.getHitDigest();
    }

    @Override
    public TransactionScopedCache createReadOnlyCache(CommitUpdate commitUpdate) {
        return create(delegate.createReadOnlyCache(commitUpdate), validationProbability, failureCallback);
    }

    private boolean shouldValidate() {
        return random.nextDouble() < validationProbability;
    }

    private void validateCacheReads(
            TableReference tableReference, Map<Cell, byte[]> remoteReads, Map<Cell, byte[]> cacheReads) {
        if (!ByteArrayUtilities.areMapsEqual(remoteReads, cacheReads)) {
            failAndLog(
                    SafeArg.of("endpoint", "get"),
                    UnsafeArg.of("table", tableReference),
                    UnsafeArg.of("remoteReads", remoteReads.keySet()),
                    UnsafeArg.of("cacheReads", cacheReads.keySet()));
        }
    }

    private void validateCacheRowReads(
            TableReference tableReference,
            NavigableMap<byte[], RowResult<byte[]>> remoteReads,
            NavigableMap<byte[], RowResult<byte[]>> cacheReads) {
        if (!ByteArrayUtilities.areRowResultsEqual(remoteReads, cacheReads)) {
            failAndLog(
                    SafeArg.of("endpoint", "getRows"),
                    UnsafeArg.of("table", tableReference),
                    UnsafeArg.of("remoteReads", remoteReads),
                    UnsafeArg.of("cacheReads", cacheReads));
        }
    }

    private void failAndLog(Arg<?>... args) {
        log.error(
                "Reading from lock watch cache returned a different result to a remote read - this indicates there "
                        + "is a corruption bug in the caching logic",
                Arrays.stream(args).collect(Collectors.toList()));
        failureCallback.run();
        throw new TransactionLockWatchFailedException(
                "Failed lock watch cache validation - will retry without caching");
    }

    private static Map<Cell, byte[]> getCells(Map<Cell, byte[]> remoteReads, Set<Cell> cells) {
        return KeyedStream.of(cells)
                .map(remoteReads::get)
                .filter(Objects::nonNull)
                .collectToMap();
    }
}
