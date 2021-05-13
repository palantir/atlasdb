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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multiset;
import com.google.common.primitives.UnsignedBytes;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CellReference;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.atlasdb.transaction.api.TransactionLockWatchFailedException;
import com.palantir.common.streams.KeyedStream;
import com.palantir.lock.AtlasCellLockDescriptor;
import com.palantir.lock.watch.CommitUpdate;
import io.vavr.collection.HashMap;
import io.vavr.collection.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Test;

public final class TransactionScopedCacheImplTest {
    private static final TableReference TABLE = TableReference.createFromFullyQualifiedName("t.table1");
    private static final Cell CELL_1 = createCell(1);
    private static final Cell CELL_2 = createCell(2);
    private static final Cell CELL_3 = createCell(3);
    private static final Cell CELL_4 = createCell(4);
    private static final Cell CELL_5 = createCell(5);
    private static final Cell CELL_6 = createCell(6);
    private static final CacheValue VALUE_1 = createValue(10);
    private static final CacheValue VALUE_2 = createValue(20);
    private static final CacheValue VALUE_3 = createValue(30);
    private static final CacheValue VALUE_4 = createValue(40);
    private static final CacheValue VALUE_5 = createValue(50);
    private static final ImmutableMap<Cell, byte[]> VALUES = ImmutableMap.<Cell, byte[]>builder()
            .put(CELL_1, VALUE_1.value().get())
            .put(CELL_2, VALUE_2.value().get())
            .put(CELL_3, VALUE_3.value().get())
            .put(CELL_4, VALUE_4.value().get())
            .put(CELL_5, VALUE_5.value().get())
            .build();
    private static final CacheValue VALUE_EMPTY = CacheValue.empty();

    private final CacheMetrics metrics = mock(CacheMetrics.class);

    @Test
    public void getReadsCachedValuesBeforeReadingFromDb() {
        TransactionScopedCache cache = TransactionScopedCacheImpl.create(snapshotWithSingleValue(), metrics);

        assertThat(getRemotelyReadCells(cache, TABLE, CELL_1, CELL_2)).containsExactlyInAnyOrder(CELL_2);
        cache.write(TABLE, ImmutableMap.of(CELL_3, VALUE_3.value().get()));

        assertThat(getRemotelyReadCells(cache, TABLE, CELL_1, CELL_2, CELL_3, CELL_4, CELL_5))
                .containsExactlyInAnyOrder(CELL_4, CELL_5);
    }

    @Test
    public void deletesCacheResultLocally() {
        TransactionScopedCache cache = TransactionScopedCacheImpl.create(snapshotWithSingleValue(), metrics);

        cache.delete(TABLE, ImmutableSet.of(CELL_1));
        assertThat(getRemotelyReadCells(cache, TABLE, CELL_1, CELL_2)).containsExactly(CELL_2);
        assertThat(cache.get(TABLE, ImmutableSet.of(CELL_1), (_unused, cells) -> remoteRead(cells)))
                .isEmpty();
    }

    @Test
    public void emptyValuesAreCachedButFilteredOutOfResults() {
        TransactionScopedCache cache = TransactionScopedCacheImpl.create(snapshotWithSingleValue(), metrics);

        assertThat(getRemotelyReadCells(cache, TABLE, CELL_1, CELL_6)).containsExactlyInAnyOrder(CELL_6);
        assertThat(getRemotelyReadCells(cache, TABLE, CELL_1, CELL_6)).isEmpty();

        assertThat(cache.get(TABLE, ImmutableSet.of(CELL_1, CELL_6), (_unused, cells) -> remoteRead(cells)))
                .containsExactlyInAnyOrderEntriesOf(
                        ImmutableMap.of(CELL_1, VALUE_1.value().get()));

        cache.finalise();
        assertThat(cache.getValueDigest().loadedValues())
                .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(CellReference.of(TABLE, CELL_6), VALUE_EMPTY));
        assertThat(cache.getHitDigest().hitCells()).containsExactly(CellReference.of(TABLE, CELL_1));
    }

    @Test
    public void lockedCellsAreNeverCached() {
        TransactionScopedCache cache = TransactionScopedCacheImpl.create(
                ValueCacheSnapshotImpl.of(
                        HashMap.of(CellReference.of(TABLE, CELL_1), CacheEntry.locked()),
                        HashSet.of(TABLE),
                        ImmutableSet.of(TABLE)),
                metrics);

        assertThat(getRemotelyReadCells(cache, TABLE, CELL_1, CELL_2)).containsExactlyInAnyOrder(CELL_1, CELL_2);
        assertThat(getRemotelyReadCells(cache, TABLE, CELL_1, CELL_2)).containsExactlyInAnyOrder(CELL_1);

        cache.finalise();
        assertThat(cache.getValueDigest().loadedValues())
                .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(CellReference.of(TABLE, CELL_2), VALUE_2));
    }

    @Test
    public void allValuesReadFromCachePreventsReadToDb() {
        TransactionScopedCache cache = TransactionScopedCacheImpl.create(snapshotWithSingleValue(), metrics);

        BiFunction<TableReference, Set<Cell>, ListenableFuture<Map<Cell, byte[]>>> valueLoader = mock(BiFunction.class);
        assertThat(cache.get(TABLE, ImmutableSet.of(CELL_1), valueLoader))
                .containsExactlyInAnyOrderEntriesOf(
                        ImmutableMap.of(CELL_1, VALUE_1.value().get()));
        verifyNoInteractions(valueLoader);
    }

    @Test
    public void getRowsUsesCellLoaderToFillPartialResults() {
        // rows 1 and 2 will have direct cell lookups, row 3 will have a row lookup
        ValueCacheSnapshot snapshot = ValueCacheSnapshotImpl.of(
                HashMap.of(
                        CellReference.of(TABLE, createCell(1, 1)),
                        CacheEntry.unlocked(createValue(createCell(1, 1))),
                        CellReference.of(TABLE, createCell(1, 2)),
                        CacheEntry.unlocked(createValue(createCell(1, 2))),
                        CellReference.of(TABLE, createCell(2, 1)),
                        CacheEntry.unlocked(createValue(createCell(2, 1)))),
                HashSet.of(TABLE),
                ImmutableSet.of(TABLE));
        TransactionScopedCache cache = TransactionScopedCacheImpl.create(snapshot);

        // add a pesky double row to make sure we do not lookup twice
        ImmutableList<byte[]> rowsAndCols =
                ImmutableList.of(createBytes(1), createBytes(2), createBytes(3), createBytes(2));
        ColumnSelection columns = ColumnSelection.create(rowsAndCols);

        Multiset<Cell> directLookups = HashMultiset.create();
        NavigableSet<byte[]> rowLookups = new TreeSet<>(UnsignedBytes.lexicographicalComparator());

        NavigableMap<byte[], RowResult<byte[]>> lookup = cache.getRows(
                TABLE,
                rowsAndCols,
                columns,
                cells -> loadCells(cells, directLookups),
                rows -> loadRows(rows, columns, rowLookups));

        assertThat(directLookups).containsExactlyInAnyOrder(createCell(1, 3), createCell(2, 2), createCell(2, 3));
        assertThat(rowLookups).containsExactly(createBytes(3));
        assertThat(lookup.size()).isEqualTo(3);
        lookup.forEach((row, rowResult) -> {
            NavigableMap<byte[], byte[]> cols = rowResult.getColumns();
            assertThat(cols.size()).isEqualTo(3);
            cols.forEach((col, val) -> assertThat(val).containsExactly(EncodingUtils.add(row, col)));
        });

        AtomicInteger additionalLookups = new AtomicInteger();
        cache.getRows(
                TABLE,
                rowsAndCols,
                columns,
                cells -> {
                    additionalLookups.incrementAndGet();
                    return loadCells(cells, directLookups);
                },
                rows -> {
                    additionalLookups.incrementAndGet();
                    return loadRows(rows, columns, rowLookups);
                });
        // all reads were cached after first call
        assertThat(additionalLookups).hasValue(0);
    }

    @Test
    public void getRowsSkipsFullyPresentRows() {
        // row 2 will have a row lookup
        ValueCacheSnapshot snapshot = ValueCacheSnapshotImpl.of(
                HashMap.of(
                        CellReference.of(TABLE, createCell(1, 1)),
                        CacheEntry.unlocked(createValue(createCell(1, 1))),
                        CellReference.of(TABLE, createCell(1, 2)),
                        CacheEntry.unlocked(createValue(createCell(1, 2))),
                        CellReference.of(TABLE, createCell(1, 3)),
                        CacheEntry.unlocked(createValue(createCell(1, 3)))),
                HashSet.of(TABLE),
                ImmutableSet.of(TABLE));
        TransactionScopedCache cache = TransactionScopedCacheImpl.create(snapshot);

        ImmutableList<byte[]> rowsAndCols = ImmutableList.of(createBytes(1), createBytes(2));
        ColumnSelection columns = ColumnSelection.create(rowsAndCols);

        Multiset<Cell> directLookups = HashMultiset.create();
        NavigableSet<byte[]> rowLookups = new TreeSet<>(UnsignedBytes.lexicographicalComparator());

        NavigableMap<byte[], RowResult<byte[]>> lookup = cache.getRows(
                TABLE,
                rowsAndCols,
                columns,
                cells -> loadCells(cells, directLookups),
                rows -> loadRows(rows, columns, rowLookups));

        assertThat(directLookups).isEmpty();
        assertThat(rowLookups).containsExactly(createBytes(2));
        assertThat(lookup.size()).isEqualTo(2);
        lookup.forEach((row, rowResult) -> {
            NavigableMap<byte[], byte[]> cols = rowResult.getColumns();
            assertThat(cols.size()).isEqualTo(2);
            cols.forEach((col, val) -> assertThat(val).containsExactly(EncodingUtils.add(row, col)));
        });
    }

    @Test
    public void getRowsRespectsEmptyLookups() {
        // row 2 will have a cell lookup
        ValueCacheSnapshot snapshot = ValueCacheSnapshotImpl.of(
                HashMap.of(
                        CellReference.of(TABLE, createCell(1, 1)),
                        CacheEntry.unlocked(CacheValue.empty()),
                        CellReference.of(TABLE, createCell(1, 2)),
                        CacheEntry.unlocked(CacheValue.empty()),
                        CellReference.of(TABLE, createCell(1, 3)),
                        CacheEntry.unlocked(CacheValue.empty()),
                        CellReference.of(TABLE, createCell(2, 1)),
                        CacheEntry.unlocked(CacheValue.empty()),
                        CellReference.of(TABLE, createCell(2, 3)),
                        CacheEntry.unlocked(CacheValue.empty())),
                HashSet.of(TABLE),
                ImmutableSet.of(TABLE));
        TransactionScopedCache cache = TransactionScopedCacheImpl.create(snapshot);

        ImmutableList<byte[]> rowsAndCols = ImmutableList.of(createBytes(1), createBytes(2), createBytes(3));
        ColumnSelection columns = ColumnSelection.create(rowsAndCols);

        Multiset<Cell> directLookups = HashMultiset.create();
        NavigableSet<byte[]> rowLookups = new TreeSet<>(UnsignedBytes.lexicographicalComparator());

        NavigableMap<byte[], RowResult<byte[]>> lookup =
                cache.getRows(TABLE, rowsAndCols, columns, cells -> loadCells(cells, directLookups), rows -> {
                    rows.forEach(rowLookups::add);
                    return new TreeMap<>(UnsignedBytes.lexicographicalComparator());
                });

        Cell onlyCell = createCell(2, 2);
        assertThat(directLookups).containsExactly(onlyCell);
        assertThat(rowLookups).containsExactly(createBytes(3));
        assertThat(lookup.size()).isEqualTo(1);
        assertThat(lookup.get(createBytes(2)).getOnlyColumnValue()).containsExactly(createBytes(onlyCell));
    }

    @Test
    public void getRowsDoesNotBreakOnEmptyRowsAndCachesThemCorrectly() {
        TransactionScopedCache cache = TransactionScopedCacheImpl.create(snapshotWithSingleValue());

        ImmutableList<byte[]> rowsAndCols = ImmutableList.of(createBytes(2), createBytes(3));
        ColumnSelection columns = ColumnSelection.create(rowsAndCols);

        Multiset<Cell> directLookups = HashMultiset.create();

        AtomicInteger rowLookups = new AtomicInteger();
        Function<Iterable<byte[]>, NavigableMap<byte[], RowResult<byte[]>>> rowLookup = rows -> {
            rowLookups.incrementAndGet();
            return new TreeMap<>(UnsignedBytes.lexicographicalComparator());
        };
        NavigableMap<byte[], RowResult<byte[]>> lookup =
                cache.getRows(TABLE, rowsAndCols, columns, cells -> loadCells(cells, directLookups), rowLookup);
        cache.getRows(TABLE, rowsAndCols, columns, cells -> loadCells(cells, directLookups), rowLookup);

        assertThat(directLookups).isEmpty();
        assertThat(lookup).isEmpty();
        assertThat(rowLookups).hasValue(1);
    }

    private static Map<Cell, byte[]> loadCells(Set<Cell> cells, Multiset<Cell> directLookups) {
        directLookups.addAll(cells);
        return KeyedStream.of(cells)
                .map(TransactionScopedCacheImplTest::createBytes)
                .collectToMap();
    }

    private static NavigableMap<byte[], RowResult<byte[]>> loadRows(
            Iterable<byte[]> rows, ColumnSelection columns, NavigableSet<byte[]> rowLookups) {
        rows.forEach(rowLookups::add);
        NavigableMap<byte[], RowResult<byte[]>> results = new TreeMap<>(UnsignedBytes.lexicographicalComparator());
        for (byte[] row : rows) {
            NavigableMap<byte[], byte[]> resultsAsMap = KeyedStream.of(columns.getSelectedColumns())
                    .map(col -> Cell.create(row, col))
                    .map(TransactionScopedCacheImplTest::createBytes)
                    .collectTo(() -> new TreeMap<>(UnsignedBytes.lexicographicalComparator()));
            results.put(row, RowResult.create(row, resultsAsMap));
        }
        return results;
    }

    @Test
    public void cacheThrowsIfReadingAfterFinalising() {
        TransactionScopedCache cache = TransactionScopedCacheImpl.create(snapshotWithSingleValue(), metrics);

        cache.finalise();
        assertThatThrownBy(() -> cache.write(TABLE, ImmutableMap.of()))
                .isExactlyInstanceOf(TransactionLockWatchFailedException.class)
                .hasMessage("Cannot get or write to a transaction scoped cache that has already been closed");

        assertThatThrownBy(() -> cache.delete(TABLE, ImmutableSet.of()))
                .isExactlyInstanceOf(TransactionLockWatchFailedException.class)
                .hasMessage("Cannot get or write to a transaction scoped cache that has already been closed");

        BiFunction<TableReference, Set<Cell>, ListenableFuture<Map<Cell, byte[]>>> noOpLoader =
                (_table, _cells) -> Futures.immediateFuture(ImmutableMap.of());
        assertThatThrownBy(() -> cache.get(TABLE, ImmutableSet.of(), noOpLoader))
                .isExactlyInstanceOf(TransactionLockWatchFailedException.class)
                .hasMessage("Cannot get or write to a transaction scoped cache that has already been closed");

        assertThatThrownBy(() -> cache.getAsync(TABLE, ImmutableSet.of(), noOpLoader))
                .isExactlyInstanceOf(TransactionLockWatchFailedException.class)
                .hasMessage("Cannot get or write to a transaction scoped cache that has already been closed");
    }

    @Test
    public void cacheThrowsIfRetrievingDigestBeforeFinalising() {
        TransactionScopedCache cache = TransactionScopedCacheImpl.create(snapshotWithSingleValue(), metrics);

        assertThatThrownBy(cache::getValueDigest)
                .isExactlyInstanceOf(TransactionLockWatchFailedException.class)
                .hasMessage("Cannot compute value or hit digest unless the cache has been finalised");

        assertThatThrownBy(cache::getHitDigest)
                .isExactlyInstanceOf(TransactionLockWatchFailedException.class)
                .hasMessage("Cannot compute value or hit digest unless the cache has been finalised");
    }

    @Test
    public void readOnlyCacheTransfersValues() {
        TransactionScopedCache cache = TransactionScopedCacheImpl.create(snapshotWithSingleValue(), metrics);

        assertThat(getRemotelyReadCells(cache, TABLE, CELL_1, CELL_2)).containsExactlyInAnyOrder(CELL_2);

        TransactionScopedCache readOnlyCache = cache.createReadOnlyCache(CommitUpdate.invalidateSome(ImmutableSet.of(
                AtlasCellLockDescriptor.of(TABLE.getQualifiedName(), CELL_1.getRowName(), CELL_1.getColumnName()))));

        assertThat(getRemotelyReadCells(readOnlyCache, TABLE, CELL_1, CELL_2, CELL_3))
                .containsExactlyInAnyOrder(CELL_1, CELL_3);
        assertThat(getRemotelyReadCells(readOnlyCache, TABLE, CELL_1, CELL_2, CELL_3))
                .containsExactlyInAnyOrder(CELL_1);
    }

    @Test
    public void loadFromDbDoesNotBlockCache() {
        ExecutorService executor = Executors.newSingleThreadExecutor();

        try {
            TransactionScopedCache cache = TransactionScopedCacheImpl.create(snapshotWithSingleValue(), metrics);
            SettableFuture<Map<Cell, byte[]>> remoteReads = SettableFuture.create();
            CountDownLatch latch = new CountDownLatch(1);

            Future<Map<Cell, byte[]>> slowRead =
                    executor.submit(() -> cache.get(TABLE, ImmutableSet.of(CELL_2), (_table, _cells) -> {
                        latch.countDown();
                        return remoteReads;
                    }));

            awaitLatch(latch);
            // This not only confirms that the read has not finished, but this is also a synchronised method, so it
            // confirms that the cache is not currently locked. Note that, currently, async values will be pushed to
            // the digest even after finalising.
            cache.finalise();
            assertThat(cache.getValueDigest().loadedValues()).isEmpty();

            remoteReads.setFuture(remoteRead(ImmutableSet.of(CELL_2)));
            Map<Cell, byte[]> reads = AtlasFutures.getUnchecked(slowRead);

            assertThat(reads)
                    .containsExactlyInAnyOrderEntriesOf(
                            ImmutableMap.of(CELL_2, VALUE_2.value().get()));
            assertThat(cache.getValueDigest().loadedValues())
                    .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(CellReference.of(TABLE, CELL_2), VALUE_2));
        } finally {
            executor.shutdown();
        }
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private static Set<Cell> getRemotelyReadCells(TransactionScopedCache cache, TableReference table, Cell... cells) {
        Set<Cell> remoteReads = new java.util.HashSet<>();
        cache.get(table, Stream.of(cells).collect(Collectors.toSet()), (_unused, cellsToRead) -> {
            remoteReads.addAll(cellsToRead);
            return remoteRead(cellsToRead);
        });
        return remoteReads;
    }

    private static ListenableFuture<Map<Cell, byte[]>> remoteRead(Set<Cell> cells) {
        return Futures.immediateFuture(KeyedStream.of(cells)
                .map(VALUES::get)
                .map(Optional::ofNullable)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collectToMap());
    }

    private static ValueCacheSnapshot snapshotWithSingleValue() {
        return ValueCacheSnapshotImpl.of(
                HashMap.of(CellReference.of(TABLE, CELL_1), CacheEntry.unlocked(VALUE_1)),
                HashSet.of(TABLE),
                ImmutableSet.of(TABLE));
    }

    private static CacheValue createValue(int value) {
        return CacheValue.of(createBytes(value));
    }

    private static CacheValue createValue(Cell cell) {
        return CacheValue.of(createBytes(cell));
    }

    private static byte[] createBytes(int value) {
        return new byte[] {(byte) value};
    }

    private static byte[] createBytes(Cell cell) {
        return EncodingUtils.add(cell.getRowName(), cell.getColumnName());
    }

    private static Cell createCell(int value) {
        return Cell.create(createBytes(value), createBytes(value + 100));
    }

    private static Cell createCell(int row, int col) {
        return Cell.create(createBytes(row), createBytes(col));
    }
}
