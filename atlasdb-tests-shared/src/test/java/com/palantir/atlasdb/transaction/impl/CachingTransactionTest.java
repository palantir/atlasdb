/*
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
    private final TableReference table = TableReference.createWithEmptyNamespace("table");
    private final Mockery m = new Mockery();
    private final Transaction t = m.mock(Transaction.class);
    private final CachingTransaction c = new CachingTransaction(t);

    @Test
    public void testCacheEmptyGets() {
        final Set<byte[]> oneRow = ImmutableSortedSet.<byte[]>orderedBy(PtBytes.BYTES_COMPARATOR).add("row".getBytes()).build();
        final ColumnSelection oneColumn = ColumnSelection.create(ImmutableList.of("c".getBytes()));
        final SortedMap<byte[], RowResult<byte[]>> emptyResults = ImmutableSortedMap.<byte[], RowResult<byte[]>>orderedBy(PtBytes.BYTES_COMPARATOR).build();

        m.checking(new Expectations() {{
            // the cache doesn't actually cache empty results in this case
            // this is probably an oversight, but this has been the behavior for a long time
            oneOf(t).getRows(table, oneRow, oneColumn); will(returnValue(emptyResults));
            oneOf(t).getRows(table, oneRow, oneColumn); will(returnValue(emptyResults));
        }});

        Assert.assertEquals(emptyResults, c.getRows(table, oneRow, oneColumn));
        Assert.assertEquals(emptyResults, c.getRows(table, oneRow, oneColumn));

        m.assertIsSatisfied();
    }

    @Test
    public void testGetRows() {
        final Set<byte[]> ONE_ROW = ImmutableSortedSet.<byte[]>orderedBy(PtBytes.BYTES_COMPARATOR).add("row".getBytes()).build();
        final ColumnSelection ONE_COLUMN = ColumnSelection.create(ImmutableList.of("col".getBytes()));

        final Set<byte[]> NO_ROWS = ImmutableSortedSet.<byte[]>orderedBy(PtBytes.BYTES_COMPARATOR).build();
        final SortedMap<byte[], RowResult<byte[]>> emptyResults = ImmutableSortedMap.<byte[], RowResult<byte[]>>orderedBy(PtBytes.BYTES_COMPARATOR).build();

        final RowResult<byte[]> rowResult = RowResult.of(Cell.create("row".getBytes(), "col".getBytes()), "value".getBytes());
        final SortedMap<byte[], RowResult<byte[]>> oneResult = ImmutableSortedMap.<byte[], RowResult<byte[]>>orderedBy(PtBytes.BYTES_COMPARATOR)
                .put("row".getBytes(), rowResult)
                .build();

        m.checking(new Expectations() {{
            // row result is cached after first call, so second call requests no rows
            oneOf(t).getRows(table, ONE_ROW, ONE_COLUMN); will(returnValue(oneResult));
            oneOf(t).getRows(table, NO_ROWS, ONE_COLUMN); will(returnValue(emptyResults));
        }});

        Assert.assertEquals(oneResult, c.getRows(table, ONE_ROW, ONE_COLUMN));
        Assert.assertEquals(oneResult, c.getRows(table, ONE_ROW, ONE_COLUMN));

        m.assertIsSatisfied();
    }

    @Test
    public void testGetCell() {

        final Cell cell = Cell.create("row".getBytes(), "c".getBytes());
        final Set<Cell> cellSet = ImmutableSet.of(cell);
        final Map<Cell, byte[]> cellValueMap = ImmutableMap.<Cell, byte[]>builder()
                .put(cell, "value".getBytes())
                .build();

        m.checking(new Expectations() {{
            // cell is cached after first call, so second call requests no cells
            oneOf(t).get(table, cellSet); will(returnValue(cellValueMap));
            oneOf(t).get(table, ImmutableSet.of()); will(returnValue(ImmutableMap.of()));
        }});

        Assert.assertEquals(cellValueMap, c.get(table, cellSet));
        Assert.assertEquals(cellValueMap, c.get(table, cellSet));

        m.assertIsSatisfied();
    }

    @Test
    public void testGetEmptyCell() {
        final Cell cell = Cell.create("row".getBytes(), "c".getBytes());
        final Set<Cell> cellSet = ImmutableSet.of(cell);
        final Map<Cell, byte[]> emptyCellValueMap = ImmutableMap.of();

        m.checking(new Expectations() {{
            // empty result is cached in this case (second call requests no cells)
            oneOf(t).get(table, cellSet); will(returnValue(emptyCellValueMap));
            oneOf(t).get(table, ImmutableSet.of()); will(returnValue(emptyCellValueMap));
        }});

        Assert.assertEquals(emptyCellValueMap, c.get(table, cellSet));
        Assert.assertEquals(emptyCellValueMap, c.get(table, cellSet));

        m.assertIsSatisfied();
    }
}
