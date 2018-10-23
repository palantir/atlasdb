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

import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.Transaction;

public class CachingTransactionTest {
    private static final byte[] ROW_BYTES = "row".getBytes();
    private static final byte[] COL_BYTES = "col".getBytes();
    private static final byte[] VALUE_BYTES = "value".getBytes();

    private final TableReference table = TableReference.createWithEmptyNamespace("table");
    private final Mockery mockery = new Mockery();
    private final Transaction txn = mockery.mock(Transaction.class);
    private final CachingTransaction ct = new CachingTransaction(txn);

    @Test
    public void testCacheEmptyGets() {
        final Set<byte[]> oneRow = ImmutableSortedSet.orderedBy(PtBytes.BYTES_COMPARATOR).add(ROW_BYTES).build();
        final ColumnSelection oneColumn = ColumnSelection.create(ImmutableList.of(COL_BYTES));
        final SortedMap<byte[], RowResult<byte[]>> emptyResults =
                ImmutableSortedMap.<byte[], RowResult<byte[]>>orderedBy(PtBytes.BYTES_COMPARATOR).build();

        mockery.checking(new Expectations() {
            {
                // the cache doesn't actually cache empty results in this case
                // this is probably an oversight, but this has been the behavior for a long time
                oneOf(txn).getRows(table, oneRow, oneColumn);
                will(returnValue(emptyResults));

                oneOf(txn).getRows(table, oneRow, oneColumn);
                will(returnValue(emptyResults));
            }
        });

        Assert.assertEquals(emptyResults, ct.getRows(table, oneRow, oneColumn));
        Assert.assertEquals(emptyResults, ct.getRows(table, oneRow, oneColumn));

        mockery.assertIsSatisfied();
    }

    @Test
    public void testGetRows() {
        final Set<byte[]> oneRow = ImmutableSortedSet.orderedBy(PtBytes.BYTES_COMPARATOR).add(ROW_BYTES).build();
        final ColumnSelection oneColumn = ColumnSelection.create(ImmutableList.of(COL_BYTES));

        final Set<byte[]> noRows = ImmutableSortedSet.orderedBy(PtBytes.BYTES_COMPARATOR).build();
        final SortedMap<byte[], RowResult<byte[]>> emptyResults =
                ImmutableSortedMap.<byte[], RowResult<byte[]>>orderedBy(PtBytes.BYTES_COMPARATOR).build();

        final RowResult<byte[]> rowResult = RowResult.of(Cell.create(ROW_BYTES, COL_BYTES), VALUE_BYTES);
        final SortedMap<byte[], RowResult<byte[]>> oneResult
                = ImmutableSortedMap.<byte[], RowResult<byte[]>>orderedBy(PtBytes.BYTES_COMPARATOR)
                .put(ROW_BYTES, rowResult)
                .build();

        mockery.checking(new Expectations() {
            {
                // row result is cached after first call, so second call requests no rows
                oneOf(txn).getRows(table, oneRow, oneColumn);
                will(returnValue(oneResult));

                oneOf(txn).getRows(table, noRows, oneColumn);
                will(returnValue(emptyResults));
            }
        });

        Assert.assertEquals(oneResult, ct.getRows(table, oneRow, oneColumn));
        Assert.assertEquals(oneResult, ct.getRows(table, oneRow, oneColumn));

        mockery.assertIsSatisfied();
    }

    @Test
    public void testGetCell() {
        final Cell cell = Cell.create(ROW_BYTES, COL_BYTES);
        final Map<Cell, byte[]> cellValueMap = ImmutableMap.<Cell, byte[]>builder()
                .put(cell, VALUE_BYTES)
                .build();

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

    private void testGetCellResults(Cell cell, Map<Cell, byte[]> cellValueMap) {
        final Set<Cell> cellSet = ImmutableSet.of(cell);
        mockery.checking(new Expectations() {
            {
                oneOf(txn).get(table, cellSet);
                will(returnValue(cellValueMap));

                oneOf(txn).get(table, ImmutableSet.of());
                will(returnValue(ImmutableMap.of()));
            }
        });

        Assert.assertEquals(cellValueMap, ct.get(table, cellSet));
        Assert.assertEquals(cellValueMap, ct.get(table, cellSet));

        mockery.assertIsSatisfied();
    }
}
