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
package com.palantir.atlasdb.keyvalue.impl;

import static java.util.Collections.emptyMap;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.commons.lang.ArrayUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.table.description.ColumnMetadataDescription;
import com.palantir.atlasdb.table.description.ColumnValueDescription;
import com.palantir.atlasdb.table.description.NameMetadataDescription;
import com.palantir.atlasdb.table.description.NamedColumnDescription;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.common.base.ClosableIterator;

public abstract class AbstractKeyValueServiceTest {
    protected static final TableReference TEST_TABLE = TableReference.createFromFullyQualifiedName("ns.pt_kvs_test");
    protected static final TableReference TEST_NONEXISTING_TABLE = TableReference.createFromFullyQualifiedName("ns2.some_nonexisting_table");

    protected static final byte[] row0 = "row0".getBytes();
    protected static final byte[] row1 = "row1".getBytes();
    protected static final byte[] row2 = "row2".getBytes();
    protected static final byte[] column0 = "column0".getBytes();
    protected static final byte[] column1 = "column1".getBytes();
    protected static final byte[] column2 = "column2".getBytes();
    protected static final byte[] column3 = "column3".getBytes();
    protected static final byte[] column4 = "column4".getBytes();
    protected static final byte[] column5 = "column5".getBytes();
    protected static final byte[] column6 = "column6".getBytes();
    protected static final byte[] value00 = "value00".getBytes();
    protected static final byte[] value01 = "value01".getBytes();
    protected static final byte[] value10 = "value10".getBytes();
    protected static final byte[] value12 = "value12".getBytes();
    protected static final byte[] value21 = "value21".getBytes();
    protected static final byte[] value22 = "value22".getBytes();

    protected static final byte[] value0_t0 = "value0_t0".getBytes();
    protected static final byte[] value0_t1 = "value1_t1".getBytes();
    protected static final byte[] value0_t5 = "value5_t5".getBytes();

    protected static final Cell TEST_CELL = Cell.create(row0, column0);
    protected static final long TEST_TIMESTAMP = 1000000L;

    protected static KeyValueService keyValueService = null;

    protected boolean reverseRangesSupported() {
        return true;
    }

    protected boolean checkAndSetSupported() {
        return true;
    }

    @Before
    public void setUp() throws Exception {
        if (keyValueService == null) {
            keyValueService = getKeyValueService();
        }
        keyValueService.createTable(TEST_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);
    }

    @After
    public void tearDown() throws Exception {
        keyValueService.truncateTables(ImmutableSet.of(TEST_TABLE));
    }

    @AfterClass
    public static void tearDownKvs() {
        if (keyValueService != null) {
            keyValueService.close();
            keyValueService = null;
        }
    }

    @Test
    public void testGetRowColumnSelection() {
        Cell cell1 = Cell.create(PtBytes.toBytes("row"), PtBytes.toBytes("col1"));
        Cell cell2 = Cell.create(PtBytes.toBytes("row"), PtBytes.toBytes("col2"));
        Cell cell3 = Cell.create(PtBytes.toBytes("row"), PtBytes.toBytes("col3"));
        byte[] val = PtBytes.toBytes("val");

        keyValueService.put(TEST_TABLE, ImmutableMap.of(cell1, val, cell2, val, cell3, val), 0);

        Map<Cell, Value> rows1 = keyValueService.getRows(
                TEST_TABLE,
                ImmutableSet.of(cell1.getRowName()),
                ColumnSelection.all(),
                1);
        Assert.assertEquals(ImmutableSet.of(cell1, cell2, cell3), rows1.keySet());

        Map<Cell, Value> rows2 = keyValueService.getRows(
                TEST_TABLE,
                ImmutableSet.of(cell1.getRowName()),
                ColumnSelection.create(ImmutableList.of(cell1.getColumnName())),
                1);
        assertEquals(ImmutableSet.of(cell1), rows2.keySet());

        Map<Cell, Value> rows3 = keyValueService.getRows(
                TEST_TABLE,
                ImmutableSet.of(cell1.getRowName()),
                ColumnSelection.create(ImmutableList.of(cell1.getColumnName(), cell3.getColumnName())),
                1);
        assertEquals(ImmutableSet.of(cell1, cell3), rows3.keySet());
        Map<Cell, Value> rows4 = keyValueService.getRows(
                TEST_TABLE,
                ImmutableSet.of(cell1.getRowName()),
                ColumnSelection.create(ImmutableList.<byte[]>of()),
                1);

        // This has changed recently - now empty column set means
        // that all columns are selected.
        assertEquals(ImmutableSet.of(cell1, cell2, cell3), rows4.keySet());
    }

    @Test
    public void testGetRowsAllColumns() {
        putTestDataForSingleTimestamp();
        Map<Cell, Value> values = keyValueService.getRows(TEST_TABLE,
                                                          Arrays.asList(row1, row2),
                                                          ColumnSelection.all(),
                                                          TEST_TIMESTAMP + 1);
        assertEquals(4, values.size());
        assertEquals(null, values.get(Cell.create(row1, column1)));
        assertArrayEquals(value10, values.get(Cell.create(row1, column0)).getContents());
        assertArrayEquals(value12, values.get(Cell.create(row1, column2)).getContents());
        assertArrayEquals(value21, values.get(Cell.create(row2, column1)).getContents());
        assertArrayEquals(value22, values.get(Cell.create(row2, column2)).getContents());
    }

    private Map<Cell, Value> getValuesForRow(Map<byte[], RowColumnRangeIterator> values, byte[] row, int number) {
        Map<Cell, Value> results = Maps.newHashMap();

        Iterator<Entry<Cell, Value>> it = Collections.emptyIterator();
        if (values.containsKey(row)) {
            it = Iterators.limit(values.get(row), number);
        }
        while (it.hasNext()) {
            Entry<Cell, Value> result = it.next();
            results.put(result.getKey(), result.getValue());
        }
        return results;
    }

    @Test
    public void testGetRowColumnRange() {
        putTestDataForSingleTimestamp();
        Map<byte[], RowColumnRangeIterator> values = keyValueService.getRowsColumnRange(TEST_TABLE,
                ImmutableList.of(row1),
                BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, 1),
                TEST_TIMESTAMP + 1);
        assertEquals(1, values.size());
        Map<Cell, Value> batchValues = getValuesForRow(values, row1, 1);
        assertEquals(1, batchValues.size());
        assertArrayEquals(batchValues.get(Cell.create(row1, column0)).getContents(), value10);
        values = keyValueService.getRowsColumnRange(TEST_TABLE,
                ImmutableList.of(row1),
                BatchColumnRangeSelection.create(RangeRequests.nextLexicographicName(column0), PtBytes.EMPTY_BYTE_ARRAY, 1),
                TEST_TIMESTAMP + 1);
        assertEquals(1, values.size());
        batchValues = getValuesForRow(values, row1, 1);
        assertEquals(1, batchValues.size());
        assertArrayEquals(batchValues.get(Cell.create(row1, column2)).getContents(), value12);
        values = keyValueService.getRowsColumnRange(TEST_TABLE,
                ImmutableList.of(row1),
                BatchColumnRangeSelection.create(RangeRequests.nextLexicographicName(column0), column2, 1),
                TEST_TIMESTAMP + 1);
        assertEquals(1, values.size());
        assertEquals(0, getValuesForRow(values, row1, 1).size());
        values = keyValueService.getRowsColumnRange(TEST_TABLE,
                ImmutableList.of(row1),
                BatchColumnRangeSelection.create(RangeRequests.nextLexicographicName(column2), PtBytes.EMPTY_BYTE_ARRAY, 1),
                TEST_TIMESTAMP + 1);
        assertEquals(1, values.size());
        assertEquals(0, getValuesForRow(values, row1, 1).size());
        values = keyValueService.getRowsColumnRange(TEST_TABLE,
                ImmutableList.of(row1),
                BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, Integer.MAX_VALUE),
                TEST_TIMESTAMP + 1);
        assertEquals(1, values.size());
        batchValues = getValuesForRow(values, row1, 2);
        assertEquals(2, batchValues.size());
        assertArrayEquals(batchValues.get(Cell.create(row1, column0)).getContents(), value10);
        assertArrayEquals(batchValues.get(Cell.create(row1, column2)).getContents(), value12);
    }

    @Test
    public void testGetRowColumnRange_pagesInOrder() {
        // reg test for bug where HashMap led to reorder, batch 3 to increase chance of that happening if HashMap change
        Map<Cell, byte[]> values = new HashMap<>();
        values.put(Cell.create(row1, column0), value10);
        values.put(Cell.create(row1, column1), value10);
        values.put(Cell.create(row1, column2), value10);
        values.put(Cell.create(row1, column3), value10);
        values.put(Cell.create(row1, column4), value10);
        values.put(Cell.create(row1, column5), value10);
        values.put(Cell.create(row1, column6), value10);
        keyValueService.put(TEST_TABLE, values, TEST_TIMESTAMP);

        RowColumnRangeIterator iterator = keyValueService.getRowsColumnRange(
                TEST_TABLE,
                ImmutableList.of(row1),
                BatchColumnRangeSelection.create(null, null, 3),
                TEST_TIMESTAMP + 1).get(row1);
        List<ByteBuffer> columns = StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
                .map(entry -> entry.getKey().getColumnName())
                .map(ByteBuffer::wrap)
                .collect(Collectors.toList());
        assertEquals(ImmutableList.of(
                ByteBuffer.wrap(column0),
                ByteBuffer.wrap(column1),
                ByteBuffer.wrap(column2),
                ByteBuffer.wrap(column3),
                ByteBuffer.wrap(column4),
                ByteBuffer.wrap(column5),
                ByteBuffer.wrap(column6)), columns);
    }

    @Test
    public void testGetRowColumnRangeHistorical() {
        putTestDataForMultipleTimestamps();
        Map<byte[], RowColumnRangeIterator> values = keyValueService.getRowsColumnRange(TEST_TABLE,
                ImmutableList.of(row0),
                BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, 1),
                TEST_TIMESTAMP + 2);
        assertEquals(1, values.size());
        Map<Cell, Value> batchValues = getValuesForRow(values, row0, 1);
        assertEquals(1, batchValues.size());
        assertArrayEquals(value0_t1, batchValues.get(TEST_CELL).getContents());
        values = keyValueService.getRowsColumnRange(TEST_TABLE,
                ImmutableList.of(row0),
                BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, 1),
                TEST_TIMESTAMP + 1);
        assertEquals(1, values.size());
        batchValues = getValuesForRow(values, row0, 1);
        assertEquals(1, batchValues.size());
        assertArrayEquals(value0_t0, batchValues.get(TEST_CELL).getContents());
    }

    @Test
    public void testGetRowColumnRangeMultipleHistorical() {
        keyValueService.put(TEST_TABLE,
                ImmutableMap.of(Cell.create(row1, column0), value0_t0), TEST_TIMESTAMP);
        keyValueService.put(TEST_TABLE,
                ImmutableMap.of(Cell.create(row1, column0), value0_t1), TEST_TIMESTAMP + 1);
        keyValueService.put(TEST_TABLE,
                ImmutableMap.of(Cell.create(row1, column1), value0_t0), TEST_TIMESTAMP);
        keyValueService.put(TEST_TABLE,
                ImmutableMap.of(Cell.create(row1, column1), value0_t1), TEST_TIMESTAMP + 1);

        // The initial multiget will get results for column0 only, then the next page for column1 will not include
        // the TEST_TIMESTAMP result so we have to get another page for column1.
        Map<byte[], RowColumnRangeIterator> values = keyValueService.getRowsColumnRange(TEST_TABLE,
                ImmutableList.of(row1),
                BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY, RangeRequests.nextLexicographicName(column1), 2),
                TEST_TIMESTAMP + 1);
        assertEquals(1, values.size());
        Map<Cell, Value> batchValues = getValuesForRow(values, row1, 2);
        assertEquals(2, batchValues.size());
        assertArrayEquals(value0_t0, batchValues.get(Cell.create(row1, column0)).getContents());
        assertArrayEquals(value0_t0, batchValues.get(Cell.create(row1, column1)).getContents());
    }

    @Test
    public void testGetRowColumnRangeMultipleRows() {
        putTestDataForSingleTimestamp();
        Map<byte[], RowColumnRangeIterator> values = keyValueService.getRowsColumnRange(TEST_TABLE,
                ImmutableList.of(row1, row0, row2),
                BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, 1),
                TEST_TIMESTAMP + 1);
        assertEquals(ImmutableSet.of(row0, row1, row2), values.keySet());
        Map<Cell, Value> row0Values = getValuesForRow(values, row0, 2);
        assertArrayEquals(value00, row0Values.get(TEST_CELL).getContents());
        assertArrayEquals(value01, row0Values.get(Cell.create(row0, column1)).getContents());
        Map<Cell, Value> row1Values = getValuesForRow(values, row1, 2);
        assertArrayEquals(value10, row1Values.get(Cell.create(row1, column0)).getContents());
        assertArrayEquals(value12, row1Values.get(Cell.create(row1, column2)).getContents());
        Map<Cell, Value> row2Values = getValuesForRow(values, row2, 2);
        assertArrayEquals(value21, row2Values.get(Cell.create(row2, column1)).getContents());
        assertArrayEquals(value22, row2Values.get(Cell.create(row2, column2)).getContents());
    }

    @Test
    public void testGetRowColumnRangeCellBatchSingleRow() {
        putTestDataForSingleTimestamp();
        RowColumnRangeIterator values = keyValueService.getRowsColumnRange(TEST_TABLE,
                ImmutableList.of(row1),
                new ColumnRangeSelection(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY),
                1,
                TEST_TIMESTAMP + 1);
        assertNextElementMatches(values, Cell.create(row1, column0), value10);
        assertNextElementMatches(values, Cell.create(row1, column2), value12);
        assertFalse(values.hasNext());
        values = keyValueService.getRowsColumnRange(TEST_TABLE,
                ImmutableList.of(row1),
                new ColumnRangeSelection(RangeRequests.nextLexicographicName(column0), PtBytes.EMPTY_BYTE_ARRAY),
                1,
                TEST_TIMESTAMP + 1);
        assertNextElementMatches(values, Cell.create(row1, column2), value12);
        assertFalse(values.hasNext());
        values = keyValueService.getRowsColumnRange(TEST_TABLE,
                ImmutableList.of(row1),
                new ColumnRangeSelection(RangeRequests.nextLexicographicName(column0), column2),
                1,
                TEST_TIMESTAMP + 1);
        assertFalse(values.hasNext());
        values = keyValueService.getRowsColumnRange(TEST_TABLE,
                ImmutableList.of(row1),
                new ColumnRangeSelection(RangeRequests.nextLexicographicName(column2), PtBytes.EMPTY_BYTE_ARRAY),
                1,
                TEST_TIMESTAMP + 1);
        assertFalse(values.hasNext());
        values = keyValueService.getRowsColumnRange(TEST_TABLE,
                ImmutableList.of(row1),
                new ColumnRangeSelection(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY),
                Integer.MAX_VALUE,
                TEST_TIMESTAMP + 1);
        assertNextElementMatches(values, Cell.create(row1, column0), value10);
        assertNextElementMatches(values, Cell.create(row1, column2), value12);
        assertFalse(values.hasNext());
    }

    @Test
    public void testGetRowColumnRangeCellBatchMultipleRows() {
        putTestDataForSingleTimestamp();
        RowColumnRangeIterator values = keyValueService.getRowsColumnRange(TEST_TABLE,
                ImmutableList.of(row1, row0, row2),
                new ColumnRangeSelection(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY),
                3,
                TEST_TIMESTAMP + 1);
        assertNextElementMatches(values, Cell.create(row1, column0), value10);
        assertNextElementMatches(values, Cell.create(row1, column2), value12);
        assertNextElementMatches(values, TEST_CELL, value00);
        assertNextElementMatches(values, Cell.create(row0, column1), value01);
        assertNextElementMatches(values, Cell.create(row2, column1), value21);
        assertNextElementMatches(values, Cell.create(row2, column2), value22);
        assertFalse(values.hasNext());
    }

    @Test
    public void testGetRowColumnRangeCellBatchMultipleRowsAndSmallerBatchHint() {
        putTestDataForSingleTimestamp();
        RowColumnRangeIterator values = keyValueService.getRowsColumnRange(TEST_TABLE,
                ImmutableList.of(row1, row0, row2),
                new ColumnRangeSelection(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY),
                2,
                TEST_TIMESTAMP + 1);
        assertNextElementMatches(values, Cell.create(row1, column0), value10);
        assertNextElementMatches(values, Cell.create(row1, column2), value12);
        assertNextElementMatches(values, TEST_CELL, value00);
        assertNextElementMatches(values, Cell.create(row0, column1), value01);
        assertNextElementMatches(values, Cell.create(row2, column1), value21);
        assertNextElementMatches(values, Cell.create(row2, column2), value22);
        assertFalse(values.hasNext());
    }

    @Test
    public void testGetRowColumnRangeCellBatchHistorical() {
        putTestDataForMultipleTimestamps();
        RowColumnRangeIterator values = keyValueService.getRowsColumnRange(TEST_TABLE,
                ImmutableList.of(row0),
                new ColumnRangeSelection(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY),
                1,
                TEST_TIMESTAMP + 2);
        assertNextElementMatches(values, TEST_CELL, value0_t1);
        assertFalse(values.hasNext());
        values = keyValueService.getRowsColumnRange(TEST_TABLE,
                ImmutableList.of(row0),
                new ColumnRangeSelection(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY),
                1,
                TEST_TIMESTAMP + 1);
        assertNextElementMatches(values, TEST_CELL, value0_t0);
        assertFalse(values.hasNext());
    }

    @Test
    public void testGetRowColumnRangeCellBatchMultipleHistorical() {
        keyValueService.put(TEST_TABLE,
                ImmutableMap.of(Cell.create(row1, column0), value0_t0), TEST_TIMESTAMP);
        keyValueService.put(TEST_TABLE,
                ImmutableMap.of(Cell.create(row1, column0), value0_t1), TEST_TIMESTAMP + 1);
        keyValueService.put(TEST_TABLE,
                ImmutableMap.of(Cell.create(row1, column1), value0_t0), TEST_TIMESTAMP);
        keyValueService.put(TEST_TABLE,
                ImmutableMap.of(Cell.create(row1, column1), value0_t1), TEST_TIMESTAMP + 1);

        RowColumnRangeIterator values = keyValueService.getRowsColumnRange(TEST_TABLE,
                ImmutableList.of(row1),
                new ColumnRangeSelection(PtBytes.EMPTY_BYTE_ARRAY, RangeRequests.nextLexicographicName(column1)),
                2,
                TEST_TIMESTAMP + 1);
        assertNextElementMatches(values, Cell.create(row1, column0), value0_t0);
        assertNextElementMatches(values, Cell.create(row1, column1), value0_t0);
        assertFalse(values.hasNext());
    }

    private static void assertNextElementMatches(RowColumnRangeIterator iterator,
                                                 Cell expectedCell,
                                                 byte[] expectedContents) {
        assertTrue("Expected next element to match, but iterator was exhausted!", iterator.hasNext());
        Map.Entry<Cell, Value> entry = iterator.next();
        assertEquals("Expected next element to match, but keys were different!", expectedCell, entry.getKey());
        assertArrayEquals("Expected next element to match, but values were different!",
                expectedContents, entry.getValue().getContents());
    }

    @Test
    public void testGetRowsWhenMultipleVersions() {
        putTestDataForMultipleTimestamps();
        Map<Cell, Value> result = keyValueService.getRows(
                TEST_TABLE,
                ImmutableSet.of(row0),
                ColumnSelection.all(),
                TEST_TIMESTAMP + 1);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(TEST_CELL));
        assertTrue(result.containsValue(Value.create(value0_t0, TEST_TIMESTAMP)));

        result = keyValueService.getRows(
                TEST_TABLE,
                ImmutableSet.of(row0),
                ColumnSelection.all(),
                TEST_TIMESTAMP + 2);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(TEST_CELL));
        assertTrue(result.containsValue(Value.create(value0_t1, TEST_TIMESTAMP + 1)));
    }

    @Test
    public void testGetRowsWhenMultipleVersionsAndColumnsSelected() {
        putTestDataForMultipleTimestamps();
        Map<Cell, Value> result = keyValueService.getRows(
                TEST_TABLE,
                ImmutableSet.of(row0),
                ColumnSelection.create(ImmutableSet.of(column0)),
                TEST_TIMESTAMP + 1);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(TEST_CELL));
        assertTrue(result.containsValue(Value.create(value0_t0, TEST_TIMESTAMP)));

        result = keyValueService.getRows(
                TEST_TABLE,
                ImmutableSet.of(row0),
                ColumnSelection.create(ImmutableSet.of(column0)),
                TEST_TIMESTAMP + 2);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(TEST_CELL));
        assertTrue(result.containsValue(Value.create(value0_t1, TEST_TIMESTAMP + 1)));
    }

    @Test
    public void testGetWhenMultipleVersions() {
        putTestDataForMultipleTimestamps();
        Value val0 = Value.create(value0_t0, TEST_TIMESTAMP);
        Value val1 = Value.create(value0_t1, TEST_TIMESTAMP + 1);

        assertTrue(keyValueService.get(TEST_TABLE, ImmutableMap.of(TEST_CELL, TEST_TIMESTAMP)).isEmpty());

        Map<Cell, Value> result = keyValueService.get(
                TEST_TABLE,
                ImmutableMap.of(TEST_CELL, TEST_TIMESTAMP + 1));
        assertTrue(result.containsKey(TEST_CELL));
        assertEquals(1, result.size());
        assertTrue(result.containsValue(val0));

        result = keyValueService.get(TEST_TABLE, ImmutableMap.of(TEST_CELL, TEST_TIMESTAMP + 2));

        assertEquals(1, result.size());
        assertTrue(result.containsKey(TEST_CELL));
        assertTrue(result.containsValue(val1));

        result = keyValueService.get(TEST_TABLE, ImmutableMap.of(TEST_CELL, TEST_TIMESTAMP + 3));

        assertEquals(1, result.size());
        assertTrue(result.containsKey(TEST_CELL));
        assertTrue(result.containsValue(val1));
    }

    @Test
    public void testGetRowsWithSelectedColumns() {
        putTestDataForSingleTimestamp();
        ColumnSelection columns1and2 = ColumnSelection.create(Arrays.asList(column1, column2));
        Map<Cell, Value> values = keyValueService.getRows(TEST_TABLE,
                                                          Arrays.asList(row1, row2),
                                                          columns1and2,
                                                          TEST_TIMESTAMP + 1);
        assertEquals(3, values.size());
        assertEquals(null, values.get(Cell.create(row1, column0)));
        assertArrayEquals(value12, values.get(Cell.create(row1, column2)).getContents());
        assertArrayEquals(value21, values.get(Cell.create(row2, column1)).getContents());
        assertArrayEquals(value22, values.get(Cell.create(row2, column2)).getContents());
    }

    @Test
    public void testGetLatestTimestamps() {
        putTestDataForMultipleTimestamps();
        Map<Cell, Long> timestamps = keyValueService.getLatestTimestamps(TEST_TABLE,
                ImmutableMap.of(TEST_CELL, TEST_TIMESTAMP + 2));
        assertTrue("Incorrect number of values returned.", timestamps.size() == 1);
        assertEquals("Incorrect value returned.", new Long(TEST_TIMESTAMP + 1),
                timestamps.get(TEST_CELL));
    }

    @Test
    public void testGetWithMultipleVersions() {
        putTestDataForMultipleTimestamps();
        Map<Cell, Value> values = keyValueService.get(TEST_TABLE,
                ImmutableMap.of(TEST_CELL, TEST_TIMESTAMP + 2));
        assertTrue("Incorrect number of values returned.", values.size() == 1);
        assertEquals("Incorrect value returned.", Value.create(value0_t1, TEST_TIMESTAMP + 1),
                values.get(TEST_CELL));
    }

    @Test
    public void testGetAllTableNames() {
        final TableReference anotherTable = TableReference.createWithEmptyNamespace("AnotherTable");
        assertEquals(1, keyValueService.getAllTableNames().size());
        assertEquals(TEST_TABLE, keyValueService.getAllTableNames().iterator().next());
        keyValueService.createTable(anotherTable, AtlasDbConstants.GENERIC_TABLE_METADATA);
        assertEquals(2, keyValueService.getAllTableNames().size());
        assertTrue(keyValueService.getAllTableNames().contains(anotherTable));
        assertTrue(keyValueService.getAllTableNames().contains(TEST_TABLE));
        keyValueService.dropTable(anotherTable);
        assertEquals(1, keyValueService.getAllTableNames().size());
        assertEquals(TEST_TABLE, keyValueService.getAllTableNames().iterator().next());
    }

    @Test
    public void testTableMetadata() {
        assertEquals(AtlasDbConstants.GENERIC_TABLE_METADATA.length, keyValueService.getMetadataForTable(TEST_TABLE).length);
        keyValueService.putMetadataForTable(TEST_TABLE, ArrayUtils.EMPTY_BYTE_ARRAY);
        assertEquals(0, keyValueService.getMetadataForTable(TEST_TABLE).length);
        keyValueService.putMetadataForTable(TEST_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);
        assertTrue(Arrays.equals(AtlasDbConstants.GENERIC_TABLE_METADATA, keyValueService.getMetadataForTable(TEST_TABLE)));
    }

    private static <V, T extends Iterator<RowResult<V>>> void assertRangeSizeAndOrdering(T it, int expectedSize, RangeRequest rangeRequest) {
        if (!it.hasNext()) {
            assertEquals(expectedSize, 0);
            return;
        }

        byte[] row = it.next().getRowName();
        int size = 1;

        final boolean reverse = rangeRequest.isReverse();
        final byte[] startRow = rangeRequest.getStartInclusive();
        final byte[] endRow = rangeRequest.getEndExclusive();

        if (startRow.length > 0) {
            if (!reverse) {
                assert UnsignedBytes.lexicographicalComparator().compare(startRow, row) <= 0;
            } else {
                assert UnsignedBytes.lexicographicalComparator().compare(startRow, row) >= 0;
            }
        }

        while (it.hasNext()) {
            byte[] nextRow = it.next().getRowName();

            if (!reverse) {
                assert UnsignedBytes.lexicographicalComparator().compare(row, nextRow) <= 0;
            } else {
                assert UnsignedBytes.lexicographicalComparator().compare(row, nextRow) >= 0;
            }

            row = nextRow;
            size++;
        }

        if (endRow.length > 0) {
            if (!reverse) {
                assert UnsignedBytes.lexicographicalComparator().compare(row, endRow) < 0;
            } else {
                assert UnsignedBytes.lexicographicalComparator().compare(row, endRow) > 0;
            }
        }

        assertEquals(expectedSize, size);
    }

    @Test
    public void testGetRange() {
        testGetRange(reverseRangesSupported());
    }

    public void testGetRange(boolean reverseSupported) {
        putTestDataForSingleTimestamp();

        // Unbounded
        final RangeRequest all = RangeRequest.all();
        assertRangeSizeAndOrdering(keyValueService.getRange(TEST_TABLE, all, TEST_TIMESTAMP + 1), 3, all);

        if (reverseSupported) {
            final RangeRequest allReverse = RangeRequest.reverseBuilder().build();
            assertRangeSizeAndOrdering(keyValueService.getRange(TEST_TABLE, allReverse, TEST_TIMESTAMP + 1), 3, allReverse);
        }

        // Upbounded
        final RangeRequest upbounded = RangeRequest.builder().endRowExclusive(row2).build();
        assertRangeSizeAndOrdering(keyValueService.getRange(TEST_TABLE, upbounded, TEST_TIMESTAMP + 1), 2, upbounded);

        if (reverseSupported) {
            final RangeRequest upboundedReverse = RangeRequest.reverseBuilder().endRowExclusive(row0).build();
            assertRangeSizeAndOrdering(keyValueService.getRange(TEST_TABLE, upboundedReverse, TEST_TIMESTAMP + 1), 2, upboundedReverse);
        }

        // Downbounded
        final RangeRequest downbounded = RangeRequest.builder().startRowInclusive(row1).build();
        assertRangeSizeAndOrdering(keyValueService.getRange(TEST_TABLE, downbounded, TEST_TIMESTAMP + 1), 2, downbounded);

        if (reverseSupported) {
            final RangeRequest downboundedReverse = RangeRequest.reverseBuilder().startRowInclusive(row1).build();
            assertRangeSizeAndOrdering(keyValueService.getRange(TEST_TABLE, downboundedReverse, TEST_TIMESTAMP + 1), 2, downboundedReverse);
        }

        // Both-bounded
        final RangeRequest bothbounded = RangeRequest.builder().startRowInclusive(row1).endRowExclusive(row2).build();
        assertRangeSizeAndOrdering(keyValueService.getRange(TEST_TABLE, bothbounded, TEST_TIMESTAMP + 1), 1, bothbounded);

        if (reverseSupported) {
            final RangeRequest bothboundedReverse = RangeRequest.reverseBuilder().startRowInclusive(row2).endRowExclusive(row1).build();
            assertRangeSizeAndOrdering(keyValueService.getRange(TEST_TABLE, bothboundedReverse, TEST_TIMESTAMP + 1), 1, bothboundedReverse);
        }

        // Precise test for lower-bounded
        RangeRequest rangeRequest = downbounded;
        ClosableIterator<RowResult<Value>> rangeResult = keyValueService.getRange(
                TEST_TABLE,
                rangeRequest,
                TEST_TIMESTAMP + 1);
        assertTrue(keyValueService.getRange(TEST_TABLE, rangeRequest, TEST_TIMESTAMP).hasNext() == false);
        assertTrue(rangeResult.hasNext());
        assertEquals(
                RowResult.create(
                        row1,
                        ImmutableSortedMap.orderedBy(UnsignedBytes.lexicographicalComparator()).put(
                                column0,
                                Value.create(value10, TEST_TIMESTAMP)).put(
                                column2,
                                Value.create(value12, TEST_TIMESTAMP)).build()),
                rangeResult.next());
        assertTrue(rangeResult.hasNext());
        assertEquals(
                RowResult.create(
                        row2,
                        ImmutableSortedMap.orderedBy(UnsignedBytes.lexicographicalComparator()).put(
                                column1,
                                Value.create(value21, TEST_TIMESTAMP)).put(
                                column2,
                                Value.create(value22, TEST_TIMESTAMP)).build()),
                rangeResult.next());
        rangeResult.close();
    }

    @Test
    public void testGetRangePaging() {
        for (int numColumnsInMetadata = 1; numColumnsInMetadata <= 3; ++numColumnsInMetadata) {
            for (int batchSizeHint = 1; batchSizeHint <= 5; ++batchSizeHint) {
                doTestGetRangePaging(numColumnsInMetadata, batchSizeHint, false);
                if (reverseRangesSupported()) {
                    doTestGetRangePaging(numColumnsInMetadata, batchSizeHint, true);
                }
            }
        }
    }

    private void doTestGetRangePaging(int numColumnsInMetadata, int batchSizeHint, boolean reverse) {
        TableReference tableRef = createTableWithNamedColumns(numColumnsInMetadata);

        Map<Cell, byte[]> values = new HashMap<Cell, byte[]>();
        values.put(Cell.create("00".getBytes(), "c1".getBytes()), "a".getBytes());
        values.put(Cell.create("00".getBytes(), "c2".getBytes()), "b".getBytes());

        values.put(Cell.create("01".getBytes(), RangeRequests.getFirstRowName()), "c".getBytes());

        values.put(Cell.create("02".getBytes(), "c1".getBytes()), "d".getBytes());
        values.put(Cell.create("02".getBytes(), "c2".getBytes()), "e".getBytes());

        values.put(Cell.create("03".getBytes(), "c1".getBytes()), "f".getBytes());

        values.put(Cell.create("04".getBytes(), "c1".getBytes()), "g".getBytes());
        values.put(Cell.create("04".getBytes(), RangeRequests.getLastRowName()), "h".getBytes());

        values.put(Cell.create("05".getBytes(), "c1".getBytes()), "i".getBytes());
        values.put(Cell.create(RangeRequests.getLastRowName(), "c1".getBytes()), "j".getBytes());
        keyValueService.put(tableRef, values, TEST_TIMESTAMP);

        RangeRequest request = RangeRequest.builder(reverse).batchHint(batchSizeHint).build();
        try (ClosableIterator<RowResult<Value>> iter = keyValueService.getRange(tableRef, request, Long.MAX_VALUE)) {
            List<RowResult<Value>> results = ImmutableList.copyOf(iter);
            List<RowResult<Value>> expected = ImmutableList.of(
                RowResult.create("00".getBytes(),
                    ImmutableSortedMap.<byte[], Value>orderedBy(UnsignedBytes.lexicographicalComparator())
                        .put("c1".getBytes(), Value.create("a".getBytes(), TEST_TIMESTAMP))
                        .put("c2".getBytes(), Value.create("b".getBytes(), TEST_TIMESTAMP))
                        .build()),
                RowResult.create("01".getBytes(),
                    ImmutableSortedMap.<byte[], Value>orderedBy(UnsignedBytes.lexicographicalComparator())
                        .put(RangeRequests.getFirstRowName(), Value.create("c".getBytes(), TEST_TIMESTAMP))
                        .build()),
                RowResult.create("02".getBytes(),
                    ImmutableSortedMap.<byte[], Value>orderedBy(UnsignedBytes.lexicographicalComparator())
                        .put("c1".getBytes(), Value.create("d".getBytes(), TEST_TIMESTAMP))
                        .put("c2".getBytes(), Value.create("e".getBytes(), TEST_TIMESTAMP))
                        .build()),
                RowResult.create("03".getBytes(),
                    ImmutableSortedMap.<byte[], Value>orderedBy(UnsignedBytes.lexicographicalComparator())
                        .put("c1".getBytes(), Value.create("f".getBytes(), TEST_TIMESTAMP))
                        .build()),
                RowResult.create("04".getBytes(),
                    ImmutableSortedMap.<byte[], Value>orderedBy(UnsignedBytes.lexicographicalComparator())
                        .put("c1".getBytes(), Value.create("g".getBytes(), TEST_TIMESTAMP))
                        .put(RangeRequests.getLastRowName(), Value.create("h".getBytes(), TEST_TIMESTAMP))
                        .build()),
                RowResult.create("05".getBytes(),
                    ImmutableSortedMap.<byte[], Value>orderedBy(UnsignedBytes.lexicographicalComparator())
                        .put("c1".getBytes(), Value.create("i".getBytes(), TEST_TIMESTAMP))
                        .build()),
                RowResult.create(RangeRequests.getLastRowName(),
                    ImmutableSortedMap.<byte[], Value>orderedBy(UnsignedBytes.lexicographicalComparator())
                        .put("c1".getBytes(), Value.create("j".getBytes(), TEST_TIMESTAMP))
                        .build())
            );
            assertEquals(reverse ? Lists.reverse(expected) : expected, results);
        }
    }

    @Test
    public void testGetRangePagingLastRowEdgeCase() {
        for (int batchSizeHint = 1; batchSizeHint <= 2; ++batchSizeHint) {
            doTestGetRangePagingLastRowEdgeCase(1, batchSizeHint, false);
            if (reverseRangesSupported()) {
                doTestGetRangePagingLastRowEdgeCase(1, batchSizeHint, true);
            }
        }
    }

    private void doTestGetRangePagingLastRowEdgeCase(int numColumnsInMetadata, int batchSizeHint, boolean reverse) {
        TableReference tableRef = createTableWithNamedColumns(numColumnsInMetadata);
        byte[] last = reverse ? RangeRequests.getFirstRowName() : RangeRequests.getLastRowName();
        Map<Cell, byte[]> values = ImmutableMap.of(
            Cell.create(last, "c1".getBytes()), "a".getBytes(),
            Cell.create(last, last), "b".getBytes());
        keyValueService.put(tableRef, values, TEST_TIMESTAMP);

        RangeRequest request = RangeRequest.builder(reverse).batchHint(batchSizeHint).build();
        try (ClosableIterator<RowResult<Value>> iter = keyValueService.getRange(tableRef, request, Long.MAX_VALUE)) {
            List<RowResult<Value>> results = ImmutableList.copyOf(iter);
            List<RowResult<Value>> expected = ImmutableList.of(
                RowResult.create(last,
                    ImmutableSortedMap.<byte[], Value>orderedBy(UnsignedBytes.lexicographicalComparator())
                            .put("c1".getBytes(), Value.create("a".getBytes(), TEST_TIMESTAMP))
                            .put(last, Value.create("b".getBytes(), TEST_TIMESTAMP))
                            .build()));
            assertEquals(expected, results);
        }
    }

    @Test
    public void testGetRangePagingWithColumnSelection() {
        for (int numRows = 1; numRows <= 6; ++numRows) {
            populateTableWithTriangularData(numRows);
            for (int numColsInSelection = 1; numColsInSelection <= 7; ++numColsInSelection) {
                for (int batchSizeHint = 1; batchSizeHint <= 7; ++batchSizeHint) {
                    doTestGetRangePagingWithColumnSelection(batchSizeHint, numRows, numColsInSelection, false);
                    if (reverseRangesSupported()) {
                        doTestGetRangePagingWithColumnSelection(batchSizeHint, numRows, numColsInSelection, true);
                    }
                }
            }
        }
    }

    // row 1: 1
    // row 2: 1 2
    // row 3: 1 2 3
    // row 4: 1 2 3 4
    // ...
    private void populateTableWithTriangularData(int numRows) {
        Map<Cell, byte[]> values = new HashMap<>();
        for (long row = 1; row <= numRows; ++row) {
            for (long col = 1; col <= row; ++col) {
                byte[] rowName = PtBytes.toBytes(Long.MIN_VALUE ^ row);
                byte[] colName = PtBytes.toBytes(Long.MIN_VALUE ^ col);
                values.put(Cell.create(rowName, colName), (row + "," + col).getBytes());
            }
        }
        keyValueService.truncateTable(TEST_TABLE);
        keyValueService.put(TEST_TABLE, values, TEST_TIMESTAMP);
    }

    private void doTestGetRangePagingWithColumnSelection(int batchSizeHint,
                                                         int numRows,
                                                         int numColsInSelection,
                                                         boolean reverse) {
        Collection<byte[]> columnSelection = new ArrayList<>(numColsInSelection);
        for (long col = 1; col <= numColsInSelection; ++col) {
            byte[] colName = PtBytes.toBytes(Long.MIN_VALUE ^ col);
            columnSelection.add(colName);
        }
        RangeRequest request = RangeRequest.builder(reverse)
                .retainColumns(columnSelection)
                .batchHint(batchSizeHint)
                .build();
        try (ClosableIterator<RowResult<Value>> iter = keyValueService.getRange(TEST_TABLE, request, Long.MAX_VALUE)) {
            List<RowResult<Value>> results = ImmutableList.copyOf(iter);
            assertEquals(getExpectedResultForRangePagingWithColumnSelectionTest(numRows, numColsInSelection, reverse),
                    results);
        }
    }

    private List<RowResult<Value>> getExpectedResultForRangePagingWithColumnSelectionTest(int numRows,
                                                                                          int numColsInSelection,
                                                                                          boolean reverse) {
        List<RowResult<Value>> expected = new ArrayList<>();
        for (long row = 1; row <= numRows; ++row) {
            ImmutableSortedMap.Builder<byte[], Value> builder = ImmutableSortedMap.orderedBy(
                    UnsignedBytes.lexicographicalComparator());
            for (long col = 1; col <= row && col <= numColsInSelection; ++col) {
                byte[] colName = PtBytes.toBytes(Long.MIN_VALUE ^ col);
                builder.put(colName, Value.create((row + "," + col).getBytes(), TEST_TIMESTAMP));
            }
            SortedMap<byte[], Value> columns = builder.build();
            if (!columns.isEmpty()) {
                byte[] rowName = PtBytes.toBytes(Long.MIN_VALUE ^ row);
                expected.add(RowResult.create(rowName, columns));
            }
        }
        if (reverse) {
            return Lists.reverse(expected);
        } else {
            return expected;
        }
    }

    @Test
    public void testGetAllTimestamps() {
        putTestDataForMultipleTimestamps();
        final Set<Cell> cellSet = ImmutableSet.of(TEST_CELL);
        Multimap<Cell, Long> timestamps = keyValueService.getAllTimestamps(
                TEST_TABLE,
                cellSet,
                TEST_TIMESTAMP);
        assertEquals(0, timestamps.size());

        timestamps = keyValueService.getAllTimestamps(TEST_TABLE, cellSet, TEST_TIMESTAMP + 1);
        assertEquals(1, timestamps.size());
        assertTrue(timestamps.containsEntry(TEST_CELL, TEST_TIMESTAMP));

        timestamps = keyValueService.getAllTimestamps(TEST_TABLE, cellSet, TEST_TIMESTAMP + 2);
        assertEquals(2, timestamps.size());
        assertTrue(timestamps.containsEntry(TEST_CELL, TEST_TIMESTAMP));
        assertTrue(timestamps.containsEntry(TEST_CELL, TEST_TIMESTAMP + 1));

        assertEquals(
                timestamps,
                keyValueService.getAllTimestamps(TEST_TABLE, cellSet, TEST_TIMESTAMP + 3));
    }

    @Test
    public void testDelete() {
        putTestDataForSingleTimestamp();
        assertEquals(3, Iterators.size(keyValueService.getRange(
                TEST_TABLE,
                RangeRequest.all(),
                TEST_TIMESTAMP + 1)));
        keyValueService.delete(
                TEST_TABLE,
                ImmutableMultimap.of(TEST_CELL, TEST_TIMESTAMP));
        assertEquals(3, Iterators.size(keyValueService.getRange(
                TEST_TABLE,
                RangeRequest.all(),
                TEST_TIMESTAMP + 1)));
        keyValueService.delete(
                TEST_TABLE,
                ImmutableMultimap.of(Cell.create(row0, column1), TEST_TIMESTAMP));
        assertEquals(2, Iterators.size(keyValueService.getRange(
                TEST_TABLE,
                RangeRequest.all(),
                TEST_TIMESTAMP + 1)));
        keyValueService.delete(
                TEST_TABLE,
                ImmutableMultimap.of(Cell.create(row1, column0), TEST_TIMESTAMP));
        assertEquals(2, Iterators.size(keyValueService.getRange(
                TEST_TABLE,
                RangeRequest.all(),
                TEST_TIMESTAMP + 1)));
        keyValueService.delete(
                TEST_TABLE,
                ImmutableMultimap.of(Cell.create(row1, column2), TEST_TIMESTAMP));
        assertEquals(1, Iterators.size(keyValueService.getRange(
                TEST_TABLE,
                RangeRequest.all(),
                TEST_TIMESTAMP + 1)));
    }

    @Test
    public void testDeleteMultipleVersions() {
        putTestDataForMultipleTimestamps();
        ClosableIterator<RowResult<Value>> result = keyValueService.getRange(
                TEST_TABLE,
                RangeRequest.all(),
                TEST_TIMESTAMP + 1);
        assertTrue(result.hasNext());

        keyValueService.delete(TEST_TABLE, ImmutableMultimap.of(TEST_CELL, TEST_TIMESTAMP));

        result = keyValueService.getRange(TEST_TABLE, RangeRequest.all(), TEST_TIMESTAMP + 1);
        assertTrue(!result.hasNext());

        result = keyValueService.getRange(TEST_TABLE, RangeRequest.all(), TEST_TIMESTAMP + 2);
        assertTrue(result.hasNext());
    }

    @Test
    public void testDeleteRangeReverse() {
        Assume.assumeTrue(reverseRangesSupported());
        setupTestRowsZeroOneAndTwoAndDeleteFrom("row1b".getBytes(), row0, true); // should delete only row1
        checkThatTableIsNowOnly(row0, row2);
    }

    @Test
    public void testDeleteRangeStartRowInclusivity() {
        setupTestRowsZeroOneAndTwoAndDeleteFrom(row0, "row1b".getBytes()); // should delete row0 and row1
        checkThatTableIsNowOnly(row2);
    }

    @Test
    public void testDeleteRangeEndRowExclusivity() {
        setupTestRowsZeroOneAndTwoAndDeleteFrom("row".getBytes(), row1); // should delete row0 only
        checkThatTableIsNowOnly(row1, row2);
    }

    @Test
    public void testDeleteRangeAll() {
        putTestDataForRowsZeroOneAndTwo();
        keyValueService.deleteRange(TEST_TABLE, RangeRequest.all());
        checkThatTableIsNowOnly();
    }

    @Test
    public void testDeleteRangeNone() {
        setupTestRowsZeroOneAndTwoAndDeleteFrom("a".getBytes(), "a".getBytes());
        checkThatTableIsNowOnly(row0, row1, row2);
    }

    private void setupTestRowsZeroOneAndTwoAndDeleteFrom(byte[] start, byte[] end) {
        setupTestRowsZeroOneAndTwoAndDeleteFrom(start, end, false);
    }

    private void setupTestRowsZeroOneAndTwoAndDeleteFrom(byte[] start, byte[] end, boolean reverse) {
        putTestDataForRowsZeroOneAndTwo();

        RangeRequest range = RangeRequest.builder(reverse)
                .startRowInclusive(start)
                .endRowExclusive(end)
                .build();
        keyValueService.deleteRange(TEST_TABLE, range);
    }

    private void checkThatTableIsNowOnly(byte[]... rows) {
        List<byte[]> keys = Lists.newArrayList();
        keyValueService.getRange(TEST_TABLE, RangeRequest.all(), AtlasDbConstants.MAX_TS)
                .forEachRemaining(row -> keys.add(row.getRowName()));
        assertTrue(Arrays.deepEquals(keys.toArray(), rows));
    }

    @Test
    public void testPutWithTimestamps() {
        putTestDataForMultipleTimestamps();
        final Value val1 = Value.create(value0_t1, TEST_TIMESTAMP + 1);
        final Value val5 = Value.create(value0_t5, TEST_TIMESTAMP + 5);
        keyValueService.putWithTimestamps(TEST_TABLE, ImmutableMultimap.of(TEST_CELL, val5));
        assertEquals(
                val5,
                keyValueService.get(TEST_TABLE, ImmutableMap.of(TEST_CELL, TEST_TIMESTAMP + 6)).get(TEST_CELL));
        assertEquals(
                val1,
                keyValueService.get(TEST_TABLE, ImmutableMap.of(TEST_CELL, TEST_TIMESTAMP + 5)).get(TEST_CELL));
        keyValueService.delete(TEST_TABLE, ImmutableMultimap.of(TEST_CELL, TEST_TIMESTAMP + 5));
    }

    @Test
    public void testGetRangeWithTimestamps() {
        testGetRangeWithTimestamps(false);
        if (reverseRangesSupported()) {
            testGetRangeWithTimestamps(true);
        }
    }

    private void testGetRangeWithTimestamps(boolean reverse) {
        putTestDataForMultipleTimestamps();
        final RangeRequest range;
        if (!reverse) {
            range = RangeRequest.builder().startRowInclusive(row0).endRowExclusive(row1).build();
        } else {
            range = RangeRequest.reverseBuilder().startRowInclusive(row0).build();
        }
        ClosableIterator<RowResult<Set<Long>>> rangeWithHistory = keyValueService.getRangeOfTimestamps(
                TEST_TABLE, range, TEST_TIMESTAMP + 2);
        RowResult<Set<Long>> row0 = rangeWithHistory.next();
        assertTrue(!rangeWithHistory.hasNext());
        rangeWithHistory.close();
        assertEquals(1, Iterables.size(row0.getCells()));
        Entry<Cell, Set<Long>> cell0 = row0.getCells().iterator().next();
        assertEquals(2, cell0.getValue().size());
        assertTrue(cell0.getValue().contains(TEST_TIMESTAMP));
        assertTrue(cell0.getValue().contains(TEST_TIMESTAMP + 1));
    }

    @Test
    public void testKeyAlreadyExists() {
        // Test that it does not throw some random exceptions
        putTestDataForSingleTimestamp();
        try {
            putTestDataForSingleTimestamp();
            // Legal
        } catch (KeyAlreadyExistsException e) {
            Assert.fail("Must not throw when overwriting with same value!");
        }

        keyValueService.putWithTimestamps(
                TEST_TABLE,
                ImmutableMultimap.of(
                        TEST_CELL,
                        Value.create(value00, TEST_TIMESTAMP + 1)));
        try {
            keyValueService.putWithTimestamps(
                    TEST_TABLE,
                    ImmutableMultimap.of(
                            TEST_CELL,
                            Value.create(value00, TEST_TIMESTAMP + 1)));
            // Legal
        } catch (KeyAlreadyExistsException e) {
            Assert.fail("Must not throw when overwriting with same value!");
        }

        try {
            keyValueService.putWithTimestamps(TEST_TABLE, ImmutableMultimap.of(
                    TEST_CELL, Value.create(value01, TEST_TIMESTAMP + 1)));
            // Legal
        } catch (KeyAlreadyExistsException e) {
            // Legal
        }

        // The first try might not throw as putUnlessExists must only be exclusive with other putUnlessExists.
        try {
            keyValueService.putUnlessExists(TEST_TABLE, ImmutableMap.of(TEST_CELL, value00));
            // Legal
        } catch (KeyAlreadyExistsException e) {
            // Legal
        }

        try {
            keyValueService.putUnlessExists(TEST_TABLE, ImmutableMap.of(TEST_CELL, value00));
            Assert.fail("putUnlessExists must throw when overwriting the same cell!");
        } catch (KeyAlreadyExistsException e) {
            // Legal
        }
    }

    @Test
    public void testCheckAndSetFromEmpty() {
        Assume.assumeTrue(checkAndSetSupported());

        CheckAndSetRequest request = CheckAndSetRequest.newCell(TEST_TABLE, TEST_CELL, value00);
        keyValueService.checkAndSet(request);

        verifyCheckAndSet(TEST_CELL, value00);
    }

    @Test
    public void testCheckAndSetFromOtherValue() {
        Assume.assumeTrue(checkAndSetSupported());

        CheckAndSetRequest request = CheckAndSetRequest.newCell(TEST_TABLE, TEST_CELL, value00);
        keyValueService.checkAndSet(request);

        CheckAndSetRequest secondRequest = CheckAndSetRequest.singleCell(TEST_TABLE, TEST_CELL, value00, value01);
        keyValueService.checkAndSet(secondRequest);

        verifyCheckAndSet(TEST_CELL, value01);
    }

    @Test
    public void testCheckAndSetAndBackAgain() {
        testCheckAndSetFromOtherValue();

        CheckAndSetRequest thirdRequest = CheckAndSetRequest.singleCell(TEST_TABLE, TEST_CELL, value01, value00);
        keyValueService.checkAndSet(thirdRequest);

        verifyCheckAndSet(TEST_CELL, value00);
    }

    private void verifyCheckAndSet(Cell key, byte[] expectedValue) {
        Multimap<Cell, Long> timestamps = keyValueService.getAllTimestamps(TEST_TABLE, ImmutableSet.of(key), 1L);

        assertEquals(1, timestamps.size());
        assertTrue(timestamps.containsEntry(key, AtlasDbConstants.TRANSACTION_TS));

        ClosableIterator<RowResult<Value>> result = keyValueService.getRange(TEST_TABLE, RangeRequest.all(),
                AtlasDbConstants.TRANSACTION_TS + 1);

        // Check first result is right
        byte[] actual = result.next().getOnlyColumnValue().getContents();
        assertArrayEquals(String.format("Value \"%s\" different from expected \"%s\"",
                new String(actual, StandardCharsets.UTF_8),
                new String(expectedValue, StandardCharsets.UTF_8)),
                expectedValue,
                actual);

        // Check no more results
        assertFalse(result.hasNext());
    }

    @Test(expected = CheckAndSetException.class)
    public void testCheckAndSetFromWrongValue() {
        Assume.assumeTrue(checkAndSetSupported());

        CheckAndSetRequest request = CheckAndSetRequest.newCell(TEST_TABLE, TEST_CELL, value00);
        keyValueService.checkAndSet(request);

        CheckAndSetRequest secondRequest = CheckAndSetRequest.singleCell(TEST_TABLE, TEST_CELL, value01, value00);
        keyValueService.checkAndSet(secondRequest);
    }

    @Test(expected = CheckAndSetException.class)
    public void testCheckAndSetFromValueWhenNoValue() {
        Assume.assumeTrue(checkAndSetSupported());

        CheckAndSetRequest request = CheckAndSetRequest.singleCell(TEST_TABLE, TEST_CELL, value00, value01);
        keyValueService.checkAndSet(request);
    }

    @Test(expected = CheckAndSetException.class)
    public void testCheckAndSetFromNoValueWhenValueIsPresent() {
        Assume.assumeTrue(checkAndSetSupported());

        CheckAndSetRequest request = CheckAndSetRequest.newCell(TEST_TABLE, TEST_CELL, value00);
        keyValueService.checkAndSet(request);
        keyValueService.checkAndSet(request);
    }

    @Test
    public void testAddGCSentinelValues() {
        putTestDataForMultipleTimestamps();

        Multimap<Cell, Long> timestampsBefore = keyValueService.getAllTimestamps(TEST_TABLE, ImmutableSet.of(TEST_CELL), AtlasDbConstants.MAX_TS);
        assertEquals(2, timestampsBefore.size());
        assertTrue(!timestampsBefore.containsEntry(TEST_CELL, Value.INVALID_VALUE_TIMESTAMP));

        keyValueService.addGarbageCollectionSentinelValues(TEST_TABLE, ImmutableSet.of(TEST_CELL));

        Multimap<Cell, Long> timestampsAfter1 = keyValueService.getAllTimestamps(TEST_TABLE, ImmutableSet.of(TEST_CELL), AtlasDbConstants.MAX_TS);
        assertEquals(3, timestampsAfter1.size());
        assertTrue(timestampsAfter1.containsEntry(TEST_CELL, Value.INVALID_VALUE_TIMESTAMP));

        keyValueService.addGarbageCollectionSentinelValues(TEST_TABLE, ImmutableSet.of(TEST_CELL));

        Multimap<Cell, Long> timestampsAfter2 = keyValueService.getAllTimestamps(TEST_TABLE, ImmutableSet.of(TEST_CELL), AtlasDbConstants.MAX_TS);
        assertEquals(3, timestampsAfter2.size());
        assertTrue(timestampsAfter2.containsEntry(TEST_CELL, Value.INVALID_VALUE_TIMESTAMP));
    }

    @Test
    public void testGetRangeThrowsOnError() {
        try {
            keyValueService.getRange(TEST_NONEXISTING_TABLE, RangeRequest.all(), AtlasDbConstants.MAX_TS).hasNext();
            Assert.fail("getRange must throw on failure");
        } catch (RuntimeException e) {
            // Expected
        }
    }

    @Test
    public void testGetRangeOfTimestampsThrowsOnError() {
        try {
            keyValueService.getRangeOfTimestamps(TEST_NONEXISTING_TABLE, RangeRequest.all(), AtlasDbConstants.MAX_TS).hasNext();
            Assert.fail("getRangeOfTimestamps must throw on failure");
        } catch (RuntimeException e) {
            // Expected
        }
    }

    @Test
    public void testCannotModifyValuesAfterWrite() {
        byte[] data = new byte[1];
        byte[] originalData = copyOf(data);
        writeToCell(TEST_CELL, data);

        modifyValue(data);

        assertThat(getForCell(TEST_CELL), is(originalData));
    }

    @Test
    public void testCannotModifyValuesAfterGetRows() {
        byte[] originalData = new byte[1];
        writeToCell(TEST_CELL, originalData);

        modifyValue(getRowsForCell(TEST_CELL));

        assertThat(getRowsForCell(TEST_CELL), is(originalData));
    }

    @Test
    public void testCannotModifyValuesAfterGet() {
        byte[] originalData = new byte[1];
        writeToCell(TEST_CELL, originalData);

        modifyValue(getForCell(TEST_CELL));

        assertThat(getForCell(TEST_CELL), is(originalData));
    }

    @Test
    public void testCannotModifyValuesAfterGetRange() {
        byte[] originalData = new byte[1];
        writeToCell(TEST_CELL, originalData);

        modifyValue(getOnlyItemInTableRange());

        assertThat(getOnlyItemInTableRange(), is(originalData));
    }

    private void modifyValue(byte[] retrievedValue) {
        retrievedValue[0] = (byte) 50;
    }

    private byte[] copyOf(byte[] contents) {
        return Arrays.copyOf(contents, contents.length);
    }

    private void writeToCell(Cell cell, byte[] data) {
        Value val = Value.create(data, TEST_TIMESTAMP + 1);
        keyValueService.putWithTimestamps(TEST_TABLE, ImmutableMultimap.of(cell, val));
    }

    private byte[] getRowsForCell(Cell cell) {
        return keyValueService.getRows(TEST_TABLE, ImmutableSet.of(cell.getRowName()), ColumnSelection.all(), TEST_TIMESTAMP + 3)
                .get(cell).getContents();
    }

    private byte[] getForCell(Cell cell) {
        return keyValueService.get(TEST_TABLE, ImmutableMap.of(cell, TEST_TIMESTAMP + 3)).get(cell).getContents();
    }

    private byte[] getOnlyItemInTableRange() {
        try (ClosableIterator<RowResult<Value>> rangeIterator =
                     keyValueService.getRange(TEST_TABLE, RangeRequest.all(), TEST_TIMESTAMP + 3) ){
            byte[] contents = rangeIterator.next().getOnlyColumnValue().getContents();

            assertFalse("There should only be one row in the table", rangeIterator.hasNext());
            return contents;
        }
    }

    @Test
    public void shouldAllowNotHavingAnyDynamicColumns() {
        keyValueService.createTable(DynamicColumnTable.reference(), DynamicColumnTable.metadata());

        byte[] row = PtBytes.toBytes(123L);
        Cell cell = Cell.create(row, dynamicColumn(1));

        Map<Cell, Long> valueToGet = ImmutableMap.of(cell, AtlasDbConstants.MAX_TS);

        assertThat(keyValueService.get(DynamicColumnTable.reference(), valueToGet), is(emptyMap()));
    }

    @Test
    public void shouldAllowRemovingAllCellsInDynamicColumns() {
        keyValueService.createTable(DynamicColumnTable.reference(), DynamicColumnTable.metadata());

        byte[] row = PtBytes.toBytes(123L);
        byte[] value = PtBytes.toBytes(123L);
        long timestamp = 456L;

        Cell cell1 = Cell.create(row, dynamicColumn(1));
        Cell cell2 = Cell.create(row, dynamicColumn(2));

        Map<Cell, Long> valuesToDelete = ImmutableMap.of(cell1, timestamp, cell2, timestamp);
        Map<Cell, byte[]> valuesToPut = ImmutableMap.of(cell1, value, cell2, value);

        keyValueService.put(DynamicColumnTable.reference(), valuesToPut, timestamp);
        keyValueService.delete(DynamicColumnTable.reference(), Multimaps.forMap(valuesToDelete));

        Map<Cell, Value> values = keyValueService.getRows(
                DynamicColumnTable.reference(),
                ImmutableList.of(row),
                ColumnSelection.all(),
                AtlasDbConstants.MAX_TS);

        assertThat(values, is(emptyMap()));
    }

    @Test
    public void shouldAllowSameTablenameDifferentNamespace() {
        TableReference fooBar = TableReference.createUnsafe("foo.bar");
        TableReference bazBar = TableReference.createUnsafe("baz.bar");

        // try create table in same call
        keyValueService.createTables(
                ImmutableMap.of(
                        fooBar, AtlasDbConstants.GENERIC_TABLE_METADATA,
                        bazBar, AtlasDbConstants.GENERIC_TABLE_METADATA));

        // try create table spanned over different calls
        keyValueService.createTable(fooBar, AtlasDbConstants.GENERIC_TABLE_METADATA);
        keyValueService.createTable(bazBar, AtlasDbConstants.GENERIC_TABLE_METADATA);

        // test tables actually created
        assertThat(keyValueService.getAllTableNames(), hasItems(fooBar, bazBar));

        // clean up
        keyValueService.dropTables(ImmutableSet.of(fooBar, bazBar));
    }

    @Test
    public void truncateShouldBeIdempotent() {
        TableReference fooBar = TableReference.createUnsafe("foo.bar");
        keyValueService.createTable(fooBar, AtlasDbConstants.GENERIC_TABLE_METADATA);

        keyValueService.truncateTable(fooBar);
        keyValueService.truncateTable(fooBar);

        keyValueService.dropTable(fooBar);
    }

    @Test
    public void truncateOfNonExistantTableShouldThrow() {
        try {
            keyValueService.truncateTable(TEST_NONEXISTING_TABLE);
            Assert.fail("truncate must throw on failure");
        } catch (RuntimeException e) {
            // expected
        }
    }

    @Test
    public void dropTableShouldBeIdempotent() {
        keyValueService.dropTable(TEST_NONEXISTING_TABLE);
        keyValueService.dropTable(TEST_NONEXISTING_TABLE);
    }

    @Test
    public void createTableShouldBeIdempotent() {
        keyValueService.createTable(TEST_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);
        keyValueService.createTable(TEST_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);
    }

    @Test
    public void compactingShouldNotFail() {
        keyValueService.compactInternally(TEST_TABLE);
    }

    private byte[] dynamicColumn(long columnId) {
        return PtBytes.toBytes(columnId);
    }

    protected void putTestDataForRowsZeroOneAndTwo() {
        keyValueService.put(TEST_TABLE,
                ImmutableMap.of(Cell.create(row0, column0), value0_t0), TEST_TIMESTAMP);
        keyValueService.put(TEST_TABLE,
            ImmutableMap.of(Cell.create(row1, column0), value0_t0), TEST_TIMESTAMP);
        keyValueService.put(TEST_TABLE,
                ImmutableMap.of(Cell.create(row2, column0), value0_t0), TEST_TIMESTAMP);
    }

    protected void putTestDataForMultipleTimestamps() {
        keyValueService.put(TEST_TABLE,
                ImmutableMap.of(TEST_CELL, value0_t0), TEST_TIMESTAMP);
        keyValueService.put(TEST_TABLE,
                ImmutableMap.of(TEST_CELL, value0_t1), TEST_TIMESTAMP + 1);
    }

    protected void putTestDataForSingleTimestamp() {
        /*      | column0     column1     column2
         * -----+---------------------------------
         * row0 | "value00"   "value01"   -
         * row1 | "value10"   -           "value12"
         * row2 | -           "value21"   "value22"
         */
        Map<Cell, byte[]> values = new HashMap<Cell, byte[]>();
        values.put(TEST_CELL, value00);
        values.put(Cell.create(row0, column1), value01);
        values.put(Cell.create(row1, column0), value10);
        values.put(Cell.create(row1, column2), value12);
        values.put(Cell.create(row2, column1), value21);
        values.put(Cell.create(row2, column2), value22);
        keyValueService.put(TEST_TABLE, values, TEST_TIMESTAMP);
    }

    private TableReference createTableWithNamedColumns(int numColumns) {
        TableReference tableRef = TableReference.createFromFullyQualifiedName(
                "ns.pt_kvs_test_named_cols_" + numColumns);
        List<NamedColumnDescription> columns = new ArrayList<>();
        for (int i = 1; i <= numColumns; ++i) {
            columns.add(new NamedColumnDescription(
                    "c" + i, "column" + i, ColumnValueDescription.forType(ValueType.BLOB)));
        }
        keyValueService.createTable(tableRef,
                new TableMetadata(
                        new NameMetadataDescription(),
                        new ColumnMetadataDescription(columns),
                        ConflictHandler.RETRY_ON_WRITE_WRITE).persistToBytes());
        keyValueService.truncateTable(tableRef);
        return tableRef;
    }

    protected abstract KeyValueService getKeyValueService();
}
