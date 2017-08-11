/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra.paging;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.KeySlice;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueServices;

public class CellPagerSplitFetchedRowsIntoTasksTest {

    private StatsAccumulator accumulator;
    private List<Double> recordedRowWidths;

    @Before
    public void mockStatsAccumulator() {
        accumulator = Mockito.mock(StatsAccumulator.class);
        recordedRowWidths = new ArrayList<>();
        Mockito.doAnswer(inv -> {
            recordedRowWidths.add(inv.getArgumentAt(0, double.class));
            return null;
        }).when(accumulator).add(Mockito.anyDouble());
    }

    @Test
    public void testTwoFullyFetchedRows() {
        // Row 100: 1
        // Row 200: 1 2
        List<Iterator<List<CassandraRawCellValue>>> iterators = CellPager.splitFetchedRowsIntoTasks(
                ImmutableList.of(keySlice(100, 1), keySlice(200, 2)),
                3,
                (rowName, lastSeenColumn) -> {
                    Assert.fail("Didn't expect this to be called - there are no partially fetch rows");
                    return null;
                },
                accumulator);
        assertEquals(1, iterators.size());
        List<List<CassandraRawCellValue>> onlyIterCopy = ImmutableList.copyOf(Iterables.getOnlyElement(iterators));
        assertEquals(
                ImmutableList.of(ImmutableList.of(rawCellValue(100, 1), rawCellValue(200, 1), rawCellValue(200, 2))),
                onlyIterCopy);
        assertEquals(ImmutableList.of(1.0, 2.0), recordedRowWidths);
    }

    @Test
    public void testSinglePartiallyFetchedRow() {
        // Row 100: 1 2 3 4 5
        List<Iterator<List<CassandraRawCellValue>>> iterators = CellPager.splitFetchedRowsIntoTasks(
                ImmutableList.of(keySlice(100, 3)),
                3,
                (rowName, lastSeenColumn) -> {
                    assertArrayEquals(key(100), rowName);
                    assertEquals(column(3), lastSeenColumn);
                    return ImmutableList.<List<ColumnOrSuperColumn>>of(
                            ImmutableList.of(columnOrSuperColumn(4), columnOrSuperColumn(5))).iterator();
                },
                accumulator);
        assertEquals(2, iterators.size());
        // The first portion of cells consists of the first three columns of our row
        assertEquals(
                ImmutableList.of(ImmutableList.of(
                        rawCellValue(100, 1), rawCellValue(100, 2), rawCellValue(100, 3))),
                ImmutableList.copyOf(iterators.get(0)));
        // The second portion of cells is the remainder of our row that we returned from
        // the mock RowRemainderIteratorFactory.
        assertEquals(
                ImmutableList.of(ImmutableList.of(rawCellValue(100, 4), rawCellValue(100, 5))),
                ImmutableList.copyOf(iterators.get(1)));
        assertEquals(ImmutableList.of(5.0), recordedRowWidths);
    }

    @Test
    public void testPartiallyFetchedRowInTheMiddle() {
        // Row 100: 1
        // Row 200: 1 2 3 4 5
        // Row 300: 1 2
        List<Iterator<List<CassandraRawCellValue>>> iterators = CellPager.splitFetchedRowsIntoTasks(
                ImmutableList.of(keySlice(100, 1), keySlice(200, 3), keySlice(300, 2)),
                3,
                (rowName, lastSeenColumn) -> {
                    assertArrayEquals(key(200), rowName);
                    assertEquals(column(3), lastSeenColumn);
                    return ImmutableList.<List<ColumnOrSuperColumn>>of(
                            ImmutableList.of(columnOrSuperColumn(4), columnOrSuperColumn(5))).iterator();
                },
                accumulator);
        assertEquals(3, iterators.size());
        // The first portion of cells includes Row 100 and the fetched part of Row 200 (cells 1, 2 and 3)
        assertEquals(
                ImmutableList.of(ImmutableList.of(
                        rawCellValue(100, 1), rawCellValue(200, 1), rawCellValue(200, 2), rawCellValue(200, 3))),
                ImmutableList.copyOf(iterators.get(0)));
        // The second portion of cells consists of the remainder of row 200 that we returned from
        // the mock RowRemainderIteratorFactory.
        assertEquals(
                ImmutableList.of(ImmutableList.of(rawCellValue(200, 4), rawCellValue(200, 5))),
                ImmutableList.copyOf(iterators.get(1)));
        // The third (and last) portion of cells is Row 300
        assertEquals(
                ImmutableList.of(ImmutableList.of(rawCellValue(300, 1), rawCellValue(300, 2))),
                ImmutableList.copyOf(iterators.get(2)));
        // We expect the widths of Rows 100 and 300 to be recorded first because they are fully fetched.
        // The width of Row 200 should be recorded only after we exhaust the corresponding iterator.
        assertEquals(ImmutableList.of(1.0, 2.0, 5.0), recordedRowWidths);
    }

    private static KeySlice keySlice(int row, int size) {
        List<ColumnOrSuperColumn> columns = new ArrayList<>(size);
        for (int col = 1; col <= size; ++col) {
            columns.add(columnOrSuperColumn(col));
        }
        return new KeySlice(ByteBuffer.wrap(key(row)), columns);
    }

    private static CassandraRawCellValue rawCellValue(int row, int col) {
        return ImmutableCassandraRawCellValue.builder()
                .rowKey(key(row))
                .column(column(col))
                .build();
    }

    private static ColumnOrSuperColumn columnOrSuperColumn(int col) {
        ColumnOrSuperColumn cosc = new ColumnOrSuperColumn();
        cosc.setColumn(column(col));
        return cosc;
    }

    private static Column column(int col) {
        Column column = new Column();
        column.setName(CassandraKeyValueServices.makeCompositeBuffer(key(col), 1000L));
        column.setValue(new byte[] { 1, 2, 3 });
        column.setTimestamp(1000L);
        return column;
    }

    private static byte[] key(int number) {
        return Ints.toByteArray(number);
    }
}
