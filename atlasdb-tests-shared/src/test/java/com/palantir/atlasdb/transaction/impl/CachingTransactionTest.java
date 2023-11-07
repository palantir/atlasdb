/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.transaction.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.util.concurrent.Futures;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.ValueAndChangeMetadata;
import com.palantir.lock.watch.ChangeMetadata;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/* TODO(boyoruk): Migrate to JUnit5 */
@RunWith(Parameterized.class)
public class CachingTransactionTest {
    private static final byte[] ROW_BYTES = "row".getBytes(StandardCharsets.UTF_8);
    private static final byte[] COL_BYTES = "col".getBytes(StandardCharsets.UTF_8);
    private static final byte[] VALUE_BYTES = "value".getBytes(StandardCharsets.UTF_8);
    private static final Cell CELL_1 = Cell.create(PtBytes.toBytes("row1"), PtBytes.toBytes("col1"));
    private static final Cell CELL_2 = Cell.create(PtBytes.toBytes("row2"), PtBytes.toBytes("col2"));
    private static final ChangeMetadata CHANGE_METADATA_1 = ChangeMetadata.unchanged();
    private static final ChangeMetadata CHANGE_METADATA_2 = ChangeMetadata.created(PtBytes.toBytes("new"));

    private static final String SYNC = "sync";
    private static final String ASYNC = "async";

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        Object[][] data = new Object[][] {
            {SYNC, UnaryOperator.identity()},
            {ASYNC, (UnaryOperator<Transaction>) GetAsyncDelegate::new}
        };
        return Arrays.asList(data);
    }

    private final TableReference table = TableReference.createWithEmptyNamespace("table");
    private final Mockery mockery = new Mockery();
    private final Transaction transaction = mockery.mock(Transaction.class);
    private final Transaction cachingTransaction;
    private final String name;
    private final Map<String, BiFunction<Set<Cell>, Map<Cell, byte[]>, Expectations>> expectationsMapping =
            ImmutableMap.<String, BiFunction<Set<Cell>, Map<Cell, byte[]>, Expectations>>builder()
                    .put(SYNC, CachingTransactionTest.this::syncGetExpectation)
                    .put(ASYNC, CachingTransactionTest.this::asyncGetExpectation)
                    .buildOrThrow();

    public CachingTransactionTest(String name, Function<Transaction, Transaction> transactionWrapper) {
        this.name = name;
        cachingTransaction = transactionWrapper.apply(new CachingTransaction(transaction));
    }

    @Test
    public void testCacheEmptyGets() {
        final Set<byte[]> oneRow = ImmutableSortedSet.orderedBy(PtBytes.BYTES_COMPARATOR)
                .add(ROW_BYTES)
                .build();
        final ColumnSelection oneColumn = ColumnSelection.create(ImmutableList.of(COL_BYTES));
        final SortedMap<byte[], RowResult<byte[]>> emptyResults =
                ImmutableSortedMap.<byte[], RowResult<byte[]>>orderedBy(PtBytes.BYTES_COMPARATOR)
                        .buildOrThrow();

        final Set<byte[]> noRows =
                ImmutableSortedSet.orderedBy(PtBytes.BYTES_COMPARATOR).build();

        mockery.checking(new Expectations() {
            {
                oneOf(transaction).getRows(table, oneRow, oneColumn);
                will(returnValue(emptyResults));

                oneOf(transaction).getRows(table, noRows, oneColumn);
                will(returnValue(emptyResults));
            }
        });

        assertThat(cachingTransaction.getRows(table, oneRow, oneColumn))
                .containsExactlyInAnyOrderEntriesOf(emptyResults);
        assertThat(cachingTransaction.getRows(table, oneRow, oneColumn))
                .containsExactlyInAnyOrderEntriesOf(emptyResults);

        mockery.assertIsSatisfied();
    }

    @Test
    public void testGetRows() {
        final Set<byte[]> oneRow = ImmutableSortedSet.orderedBy(PtBytes.BYTES_COMPARATOR)
                .add(ROW_BYTES)
                .build();
        final ColumnSelection oneColumn = ColumnSelection.create(ImmutableList.of(COL_BYTES));

        final Set<byte[]> noRows =
                ImmutableSortedSet.orderedBy(PtBytes.BYTES_COMPARATOR).build();
        final SortedMap<byte[], RowResult<byte[]>> emptyResults =
                ImmutableSortedMap.<byte[], RowResult<byte[]>>orderedBy(PtBytes.BYTES_COMPARATOR)
                        .buildOrThrow();

        final RowResult<byte[]> rowResult = RowResult.of(Cell.create(ROW_BYTES, COL_BYTES), VALUE_BYTES);
        final SortedMap<byte[], RowResult<byte[]>> oneResult = ImmutableSortedMap.<byte[], RowResult<byte[]>>orderedBy(
                        PtBytes.BYTES_COMPARATOR)
                .put(ROW_BYTES, rowResult)
                .buildOrThrow();

        mockery.checking(new Expectations() {
            {
                // row result is cached after first call, so second call requests no rows
                oneOf(transaction).getRows(table, oneRow, oneColumn);
                will(returnValue(oneResult));

                oneOf(transaction).getRows(table, noRows, oneColumn);
                will(returnValue(emptyResults));
            }
        });

        assertThat(cachingTransaction.getRows(table, oneRow, oneColumn)).containsExactlyInAnyOrderEntriesOf(oneResult);
        assertThat(cachingTransaction.getRows(table, oneRow, oneColumn)).containsExactlyInAnyOrderEntriesOf(oneResult);

        mockery.assertIsSatisfied();
    }

    @Test
    public void testGetCell() {
        final Cell cell = Cell.create(ROW_BYTES, COL_BYTES);
        final Map<Cell, byte[]> cellValueMap =
                ImmutableMap.<Cell, byte[]>builder().put(cell, VALUE_BYTES).buildOrThrow();

        // cell is cached after first call, so second call requests no cells
        testGetCellResults(cell, cellValueMap);
    }

    @Test
    public void testGetEmptyCell() {
        final Cell cell = Cell.create(ROW_BYTES, COL_BYTES);
        final Map<Cell, byte[]> emptyCellValueMap = ImmutableMap.of();

        // empty result is cached in this case (second call requests no cells)
        testGetCellResults(cell, emptyCellValueMap);
    }

    @Test
    public void metadataIsForwardedForPutWithMetadata() {
        Transaction txn = mock(Transaction.class);
        CachingTransaction cachingTransaction = new CachingTransaction(txn);
        cachingTransaction.putWithMetadata(
                table,
                ImmutableMap.of(
                        CELL_1,
                        ValueAndChangeMetadata.of(VALUE_BYTES, CHANGE_METADATA_1),
                        CELL_2,
                        ValueAndChangeMetadata.of(VALUE_BYTES, CHANGE_METADATA_2)));
        verify(txn)
                .putWithMetadata(
                        table,
                        ImmutableMap.of(
                                CELL_1,
                                ValueAndChangeMetadata.of(VALUE_BYTES, CHANGE_METADATA_1),
                                CELL_2,
                                ValueAndChangeMetadata.of(VALUE_BYTES, CHANGE_METADATA_2)));
    }

    @Test
    public void metadataIsForwardedForDeleteWithMetadata() {
        Transaction txn = mock(Transaction.class);
        CachingTransaction cachingTransaction = new CachingTransaction(txn);
        cachingTransaction.deleteWithMetadata(
                table, ImmutableMap.of(CELL_1, CHANGE_METADATA_1, CELL_2, CHANGE_METADATA_2));
        verify(txn).deleteWithMetadata(table, ImmutableMap.of(CELL_1, CHANGE_METADATA_1, CELL_2, CHANGE_METADATA_2));
    }

    private void testGetCellResults(Cell cell, Map<Cell, byte[]> cellValueMap) {
        final Set<Cell> cellSet = ImmutableSet.of(cell);
        mockery.checking(expectationsMapping.get(name).apply(cellSet, cellValueMap));

        assertThat(cachingTransaction.get(table, cellSet)).containsExactlyInAnyOrderEntriesOf(cellValueMap);
        assertThat(cachingTransaction.get(table, cellSet)).containsExactlyInAnyOrderEntriesOf(cellValueMap);

        mockery.assertIsSatisfied();
    }

    private Expectations syncGetExpectation(Set<Cell> cellSet, Map<Cell, byte[]> cellValueMap) {
        return new Expectations() {
            {
                oneOf(transaction).get(table, cellSet);
                will(returnValue(cellValueMap));

                oneOf(transaction).get(table, ImmutableSet.of());
                will(returnValue(ImmutableMap.of()));
            }
        };
    }

    private Expectations asyncGetExpectation(Set<Cell> cellSet, Map<Cell, byte[]> cellValueMap) {
        return new Expectations() {
            {
                oneOf(transaction).getAsync(table, cellSet);
                will(returnValue(Futures.immediateFuture(cellValueMap)));

                oneOf(transaction).getAsync(table, ImmutableSet.of());
                will(returnValue(Futures.immediateFuture(ImmutableMap.of())));
            }
        };
    }
}
