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
package com.palantir.atlasdb.keyvalue.impl;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
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
import java.util.stream.Collectors;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.After;
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
import com.google.common.collect.Streams;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetCompatibility;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.ClusterAvailabilityStatus;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.TimestampRangeDelete;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.LogSafety;
import com.palantir.atlasdb.table.description.ColumnMetadataDescription;
import com.palantir.atlasdb.table.description.ColumnValueDescription;
import com.palantir.atlasdb.table.description.NamedColumnDescription;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.exception.AtlasDbDependencyException;

public abstract class AbstractKeyValueServiceTest {
    private final KvsManager kvsManager;

    protected static final TableReference TEST_TABLE = TableReference.createFromFullyQualifiedName("ns.pt_kvs_test");
    private static final TableReference TEST_NONEXISTING_TABLE = TableReference.createFromFullyQualifiedName(
            "ns2.some_nonexisting_table");

    private static final Cell TEST_CELL = Cell.create(row(0), column(0));
    private static final long TEST_TIMESTAMP = 1000000L;

    protected KeyValueService keyValueService;

    protected AbstractKeyValueServiceTest(KvsManager kvsManager) {
        this.kvsManager = kvsManager;
    }

    protected boolean reverseRangesSupported() {
        return true;
    }

    protected boolean checkAndSetSupported() {
        return true;
    }

    @Before
    public void setUp() throws Exception {
        keyValueService = kvsManager.getDefaultKvs();
        keyValueService.createTable(TEST_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);
    }

    @After
    public void tearDown() throws Exception {
        try {
            keyValueService.truncateTables(ImmutableSet.of(TEST_TABLE));
        } catch (Exception e) {
            // this is fine
        }
    }

    @Test
    public void supportsCheckAndSetIsConsistentWithExpectations() {
        assertThat(keyValueService.supportsCheckAndSet(), is(checkAndSetSupported()));
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
                ColumnSelection.create(ImmutableList.of()),
                1);

        // This has changed recently - now empty column set means
        // that all columns are selected.
        assertEquals(ImmutableSet.of(cell1, cell2, cell3), rows4.keySet());
    }

    @Test
    public void testGetRowsAllColumns() {
        putTestDataForSingleTimestamp();
        Map<Cell, Value> values = keyValueService.getRows(TEST_TABLE,
                                                          Arrays.asList(row(1), row(2)),
                                                          ColumnSelection.all(),
                                                          TEST_TIMESTAMP + 1);
        assertEquals(4, values.size());
        assertNull(values.get(Cell.create(row(1), column(1))));
        assertArrayEquals(val(1, 0), values.get(Cell.create(row(1), column(0))).getContents());
        assertArrayEquals(val(1, 2), values.get(Cell.create(row(1), column(2))).getContents());
        assertArrayEquals(val(2, 1), values.get(Cell.create(row(2), column(1))).getContents());
        assertArrayEquals(val(2, 2), values.get(Cell.create(row(2), column(2))).getContents());
    }

    private Map<Cell, Value> getValuesForRow(Map<byte[], RowColumnRangeIterator> values, byte[] row, int number) {
        Map<Cell, Value> results = Maps.newHashMap();

        Iterator<Entry<Cell, Value>> it = values.entrySet().stream()
                .filter(entry -> Arrays.equals(row, entry.getKey()))
                .findFirst()
                .map(Entry::getValue)
                .map(value -> Iterators.limit(value, number))
                .orElseGet(Collections::emptyIterator);

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
                ImmutableList.of(row(1)),
                BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, 1),
                TEST_TIMESTAMP + 1);
        assertEquals(1, values.size());
        Map<Cell, Value> batchValues = getValuesForRow(values, row(1), 1);
        assertEquals(1, batchValues.size());
        assertArrayEquals(batchValues.get(Cell.create(row(1), column(0))).getContents(), val(1, 0));
        values = keyValueService.getRowsColumnRange(TEST_TABLE,
                ImmutableList.of(row(1)),
                BatchColumnRangeSelection.create(RangeRequests.nextLexicographicName(column(0)),
                        PtBytes.EMPTY_BYTE_ARRAY,
                        1),
                TEST_TIMESTAMP + 1);
        assertEquals(1, values.size());
        batchValues = getValuesForRow(values, row(1), 1);
        assertEquals(1, batchValues.size());
        assertArrayEquals(batchValues.get(Cell.create(row(1), column(2))).getContents(), val(1, 2));
        values = keyValueService.getRowsColumnRange(TEST_TABLE,
                ImmutableList.of(row(1)),
                BatchColumnRangeSelection.create(RangeRequests.nextLexicographicName(column(0)), column(2), 1),
                TEST_TIMESTAMP + 1);
        assertEquals(1, values.size());
        assertEquals(0, getValuesForRow(values, row(1), 1).size());
        values = keyValueService.getRowsColumnRange(TEST_TABLE,
                ImmutableList.of(row(1)),
                BatchColumnRangeSelection.create(RangeRequests.nextLexicographicName(column(2)),
                        PtBytes.EMPTY_BYTE_ARRAY,
                        1),
                TEST_TIMESTAMP + 1);
        assertEquals(1, values.size());
        assertEquals(0, getValuesForRow(values, row(1), 1).size());
        values = keyValueService.getRowsColumnRange(TEST_TABLE,
                ImmutableList.of(row(1)),
                BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, Integer.MAX_VALUE),
                TEST_TIMESTAMP + 1);
        assertEquals(1, values.size());
        batchValues = getValuesForRow(values, row(1), 2);
        assertEquals(2, batchValues.size());
        assertArrayEquals(batchValues.get(Cell.create(row(1), column(0))).getContents(), val(1, 0));
        assertArrayEquals(batchValues.get(Cell.create(row(1), column(2))).getContents(), val(1, 2));
    }

    @Test
    public void testGetRowColumnRange_pagesInOrder() {
        // reg test for bug where HashMap led to reorder, batch 3 to increase chance of that happening if HashMap change
        Map<Cell, byte[]> values = new HashMap<>();
        values.put(Cell.create(row(1), column(0)), val(1, 0));
        values.put(Cell.create(row(1), column(1)), val(1, 0));
        values.put(Cell.create(row(1), column(2)), val(1, 0));
        values.put(Cell.create(row(1), column(3)), val(1, 0));
        values.put(Cell.create(row(1), column(4)), val(1, 0));
        values.put(Cell.create(row(1), column(5)), val(1, 0));
        values.put(Cell.create(row(1), column(6)), val(1, 0));
        keyValueService.put(TEST_TABLE, values, TEST_TIMESTAMP);

        Map<byte[], RowColumnRangeIterator> result = keyValueService.getRowsColumnRange(
                TEST_TABLE,
                ImmutableList.of(row(1)),
                BatchColumnRangeSelection.create(null, null, 3),
                TEST_TIMESTAMP + 1);
        RowColumnRangeIterator iterator = result.entrySet().stream()
                .filter(entry -> Arrays.equals(row(1), entry.getKey()))
                .findFirst().get()
                .getValue();
        List<ByteBuffer> columns = Streams.stream(iterator)
                .map(entry -> entry.getKey().getColumnName())
                .map(ByteBuffer::wrap)
                .collect(Collectors.toList());
        assertEquals(ImmutableList.of(
                ByteBuffer.wrap(column(0)),
                ByteBuffer.wrap(column(1)),
                ByteBuffer.wrap(column(2)),
                ByteBuffer.wrap(column(3)),
                ByteBuffer.wrap(column(4)),
                ByteBuffer.wrap(column(5)),
                ByteBuffer.wrap(column(6))), columns);
    }

    @Test
    public void testGetRowColumnRangeHistorical() {
        putTestDataForMultipleTimestamps();
        Map<byte[], RowColumnRangeIterator> values = keyValueService.getRowsColumnRange(TEST_TABLE,
                ImmutableList.of(row(0)),
                BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, 1),
                TEST_TIMESTAMP + 2);
        assertEquals(1, values.size());
        Map<Cell, Value> batchValues = getValuesForRow(values, row(0), 1);
        assertEquals(1, batchValues.size());
        assertArrayEquals(val(0, 7), batchValues.get(TEST_CELL).getContents());
        values = keyValueService.getRowsColumnRange(TEST_TABLE,
                ImmutableList.of(row(0)),
                BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, 1),
                TEST_TIMESTAMP + 1);
        assertEquals(1, values.size());
        batchValues = getValuesForRow(values, row(0), 1);
        assertEquals(1, batchValues.size());
        assertArrayEquals(val(0, 5), batchValues.get(TEST_CELL).getContents());
    }

    @Test
    public void testGetRowColumnRangeMultipleHistorical() {
        keyValueService.put(TEST_TABLE,
                ImmutableMap.of(Cell.create(row(1), column(0)), val(0, 5)), TEST_TIMESTAMP);
        keyValueService.put(TEST_TABLE,
                ImmutableMap.of(Cell.create(row(1), column(0)), val(0, 7)), TEST_TIMESTAMP + 1);
        keyValueService.put(TEST_TABLE,
                ImmutableMap.of(Cell.create(row(1), column(1)), val(0, 5)), TEST_TIMESTAMP);
        keyValueService.put(TEST_TABLE,
                ImmutableMap.of(Cell.create(row(1), column(1)), val(0, 7)), TEST_TIMESTAMP + 1);

        // The initial multiget will get results for column0 only, then the next page for column1 will not include
        // the TEST_TIMESTAMP result so we have to get another page for column1.
        Map<byte[], RowColumnRangeIterator> values = keyValueService.getRowsColumnRange(TEST_TABLE,
                ImmutableList.of(row(1)),
                BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY,
                        RangeRequests.nextLexicographicName(column(1)),
                        2),
                TEST_TIMESTAMP + 1);
        assertEquals(1, values.size());
        Map<Cell, Value> batchValues = getValuesForRow(values, row(1), 2);
        assertEquals(2, batchValues.size());
        assertArrayEquals(val(0, 5), batchValues.get(Cell.create(row(1), column(0))).getContents());
        assertArrayEquals(val(0, 5), batchValues.get(Cell.create(row(1), column(1))).getContents());
    }

    @Test
    public void testGetRowColumnRangeMultipleRows() {
        putTestDataForSingleTimestamp();
        Map<byte[], RowColumnRangeIterator> values = keyValueService.getRowsColumnRange(TEST_TABLE,
                ImmutableList.of(row(1), row(0), row(2)),
                BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, 1),
                TEST_TIMESTAMP + 1);
        assertEquals(3, values.keySet().size());
        Map<Cell, Value> row0Values = getValuesForRow(values, row(0), 2);
        assertArrayEquals(val(0, 0), row0Values.get(TEST_CELL).getContents());
        assertArrayEquals(val(0, 1), row0Values.get(Cell.create(row(0), column(1))).getContents());
        Map<Cell, Value> row1Values = getValuesForRow(values, row(1), 2);
        assertArrayEquals(val(1, 0), row1Values.get(Cell.create(row(1), column(0))).getContents());
        assertArrayEquals(val(1, 2), row1Values.get(Cell.create(row(1), column(2))).getContents());
        Map<Cell, Value> row2Values = getValuesForRow(values, row(2), 2);
        assertArrayEquals(val(2, 1), row2Values.get(Cell.create(row(2), column(1))).getContents());
        assertArrayEquals(val(2, 2), row2Values.get(Cell.create(row(2), column(2))).getContents());
    }

    @Test
    public void testGetRowColumnRangeCellBatchSingleRow() {
        putTestDataForSingleTimestamp();
        RowColumnRangeIterator values = keyValueService.getRowsColumnRange(TEST_TABLE,
                ImmutableList.of(row(1)),
                new ColumnRangeSelection(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY),
                1,
                TEST_TIMESTAMP + 1);
        assertNextElementMatches(values, Cell.create(row(1), column(0)), val(1, 0));
        assertNextElementMatches(values, Cell.create(row(1), column(2)), val(1, 2));
        assertFalse(values.hasNext());
        values = keyValueService.getRowsColumnRange(TEST_TABLE,
                ImmutableList.of(row(1)),
                new ColumnRangeSelection(RangeRequests.nextLexicographicName(column(0)), PtBytes.EMPTY_BYTE_ARRAY),
                1,
                TEST_TIMESTAMP + 1);
        assertNextElementMatches(values, Cell.create(row(1), column(2)), val(1, 2));
        assertFalse(values.hasNext());
        values = keyValueService.getRowsColumnRange(TEST_TABLE,
                ImmutableList.of(row(1)),
                new ColumnRangeSelection(RangeRequests.nextLexicographicName(column(0)), column(2)),
                1,
                TEST_TIMESTAMP + 1);
        assertFalse(values.hasNext());
        values = keyValueService.getRowsColumnRange(TEST_TABLE,
                ImmutableList.of(row(1)),
                new ColumnRangeSelection(RangeRequests.nextLexicographicName(column(2)), PtBytes.EMPTY_BYTE_ARRAY),
                1,
                TEST_TIMESTAMP + 1);
        assertFalse(values.hasNext());
        values = keyValueService.getRowsColumnRange(TEST_TABLE,
                ImmutableList.of(row(1)),
                new ColumnRangeSelection(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY),
                Integer.MAX_VALUE,
                TEST_TIMESTAMP + 1);
        assertNextElementMatches(values, Cell.create(row(1), column(0)), val(1, 0));
        assertNextElementMatches(values, Cell.create(row(1), column(2)), val(1, 2));
        assertFalse(values.hasNext());
    }

    @Test
    public void testGetRowColumnRangeCellBatchMultipleRows() {
        putTestDataForSingleTimestamp();
        RowColumnRangeIterator values = keyValueService.getRowsColumnRange(TEST_TABLE,
                ImmutableList.of(row(1), row(0), row(2)),
                new ColumnRangeSelection(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY),
                3,
                TEST_TIMESTAMP + 1);
        assertNextElementMatches(values, Cell.create(row(1), column(0)), val(1, 0));
        assertNextElementMatches(values, Cell.create(row(1), column(2)), val(1, 2));
        assertNextElementMatches(values, TEST_CELL, val(0, 0));
        assertNextElementMatches(values, Cell.create(row(0), column(1)), val(0, 1));
        assertNextElementMatches(values, Cell.create(row(2), column(1)), val(2, 1));
        assertNextElementMatches(values, Cell.create(row(2), column(2)), val(2, 2));
        assertFalse(values.hasNext());
    }

    @Test
    public void testGetRowColumnRangeCellBatchMultipleRowsAndSmallerBatchHint() {
        putTestDataForSingleTimestamp();
        RowColumnRangeIterator values = keyValueService.getRowsColumnRange(TEST_TABLE,
                ImmutableList.of(row(1), row(0), row(2)),
                new ColumnRangeSelection(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY),
                2,
                TEST_TIMESTAMP + 1);
        assertNextElementMatches(values, Cell.create(row(1), column(0)), val(1, 0));
        assertNextElementMatches(values, Cell.create(row(1), column(2)), val(1, 2));
        assertNextElementMatches(values, TEST_CELL, val(0, 0));
        assertNextElementMatches(values, Cell.create(row(0), column(1)), val(0, 1));
        assertNextElementMatches(values, Cell.create(row(2), column(1)), val(2, 1));
        assertNextElementMatches(values, Cell.create(row(2), column(2)), val(2, 2));
        assertFalse(values.hasNext());
    }

    @Test
    public void testGetRowColumnRangeCellBatchHistorical() {
        putTestDataForMultipleTimestamps();
        RowColumnRangeIterator values = keyValueService.getRowsColumnRange(TEST_TABLE,
                ImmutableList.of(row(0)),
                new ColumnRangeSelection(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY),
                1,
                TEST_TIMESTAMP + 2);
        assertNextElementMatches(values, TEST_CELL, val(0, 7));
        assertFalse(values.hasNext());
        values = keyValueService.getRowsColumnRange(TEST_TABLE,
                ImmutableList.of(row(0)),
                new ColumnRangeSelection(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY),
                1,
                TEST_TIMESTAMP + 1);
        assertNextElementMatches(values, TEST_CELL, val(0, 5));
        assertFalse(values.hasNext());
    }

    @Test
    public void testGetRowColumnRangeCellBatchMultipleHistorical() {
        keyValueService.put(TEST_TABLE,
                ImmutableMap.of(Cell.create(row(1), column(0)), val(0, 5)), TEST_TIMESTAMP);
        keyValueService.put(TEST_TABLE,
                ImmutableMap.of(Cell.create(row(1), column(0)), val(0, 7)), TEST_TIMESTAMP + 1);
        keyValueService.put(TEST_TABLE,
                ImmutableMap.of(Cell.create(row(1), column(1)), val(0, 5)), TEST_TIMESTAMP);
        keyValueService.put(TEST_TABLE,
                ImmutableMap.of(Cell.create(row(1), column(1)), val(0, 7)), TEST_TIMESTAMP + 1);

        RowColumnRangeIterator values = keyValueService.getRowsColumnRange(TEST_TABLE,
                ImmutableList.of(row(1)),
                new ColumnRangeSelection(PtBytes.EMPTY_BYTE_ARRAY, RangeRequests.nextLexicographicName(column(1))),
                2,
                TEST_TIMESTAMP + 1);
        assertNextElementMatches(values, Cell.create(row(1), column(0)), val(0, 5));
        assertNextElementMatches(values, Cell.create(row(1), column(1)), val(0, 5));
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
                ImmutableSet.of(row(0)),
                ColumnSelection.all(),
                TEST_TIMESTAMP + 1);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(TEST_CELL));
        assertTrue(result.containsValue(Value.create(val(0, 5), TEST_TIMESTAMP)));

        result = keyValueService.getRows(
                TEST_TABLE,
                ImmutableSet.of(row(0)),
                ColumnSelection.all(),
                TEST_TIMESTAMP + 2);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(TEST_CELL));
        assertTrue(result.containsValue(Value.create(val(0, 7), TEST_TIMESTAMP + 1)));
    }

    @Test
    public void testGetRowsWhenMultipleVersionsAndColumnsSelected() {
        putTestDataForMultipleTimestamps();
        Map<Cell, Value> result = keyValueService.getRows(
                TEST_TABLE,
                ImmutableSet.of(row(0)),
                ColumnSelection.create(ImmutableSet.of(column(0))),
                TEST_TIMESTAMP + 1);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(TEST_CELL));
        assertTrue(result.containsValue(Value.create(val(0, 5), TEST_TIMESTAMP)));

        result = keyValueService.getRows(
                TEST_TABLE,
                ImmutableSet.of(row(0)),
                ColumnSelection.create(ImmutableSet.of(column(0))),
                TEST_TIMESTAMP + 2);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(TEST_CELL));
        assertTrue(result.containsValue(Value.create(val(0, 7), TEST_TIMESTAMP + 1)));
    }

    @Test
    public void testGetWhenMultipleVersions() {
        putTestDataForMultipleTimestamps();
        Value val0 = Value.create(val(0, 5), TEST_TIMESTAMP);
        Value val1 = Value.create(val(0, 7), TEST_TIMESTAMP + 1);

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
        ColumnSelection columns1and2 = ColumnSelection.create(Arrays.asList(column(1), column(2)));
        Map<Cell, Value> values = keyValueService.getRows(TEST_TABLE,
                                                          Arrays.asList(row(1), row(2)),
                                                          columns1and2,
                                                          TEST_TIMESTAMP + 1);
        assertEquals(3, values.size());
        assertNull(values.get(Cell.create(row(1), column(0))));
        assertArrayEquals(val(1, 2), values.get(Cell.create(row(1), column(2))).getContents());
        assertArrayEquals(val(2, 1), values.get(Cell.create(row(2), column(1))).getContents());
        assertArrayEquals(val(2, 2), values.get(Cell.create(row(2), column(2))).getContents());
    }

    @Test
    public void testGetLatestTimestamps() {
        putTestDataForMultipleTimestamps();
        Map<Cell, Long> timestamps = keyValueService.getLatestTimestamps(TEST_TABLE,
                ImmutableMap.of(TEST_CELL, TEST_TIMESTAMP + 2));
        assertEquals("Incorrect number of values returned.", 1, timestamps.size());
        assertEquals("Incorrect value returned.", Long.valueOf(TEST_TIMESTAMP + 1),
                timestamps.get(TEST_CELL));
    }

    @Test
    public void testGetWithMultipleVersions() {
        putTestDataForMultipleTimestamps();
        Map<Cell, Value> values = keyValueService.get(TEST_TABLE,
                ImmutableMap.of(TEST_CELL, TEST_TIMESTAMP + 2));
        assertEquals("Incorrect number of values returned.", 1, values.size());
        assertEquals("Incorrect value returned.", Value.create(val(0, 7), TEST_TIMESTAMP + 1),
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
        assertEquals(AtlasDbConstants.GENERIC_TABLE_METADATA.length,
                keyValueService.getMetadataForTable(TEST_TABLE).length);
        keyValueService.putMetadataForTable(TEST_TABLE, ArrayUtils.EMPTY_BYTE_ARRAY);
        assertEquals(0, keyValueService.getMetadataForTable(TEST_TABLE).length);
        keyValueService.putMetadataForTable(TEST_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);
        assertTrue(Arrays.equals(AtlasDbConstants.GENERIC_TABLE_METADATA,
                keyValueService.getMetadataForTable(TEST_TABLE)));
    }

    @Test
    public void testDropTableErasesMetadata() {
        keyValueService.dropTable(TEST_TABLE);
        assertEquals(0, keyValueService.getMetadataForTable(TEST_TABLE).length);
    }

    private static <V, T extends Iterator<RowResult<V>>> void assertRangeSizeAndOrdering(T it,
            int expectedSize,
            RangeRequest rangeRequest) {
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
            assertRangeSizeAndOrdering(keyValueService.getRange(TEST_TABLE, allReverse, TEST_TIMESTAMP + 1),
                    3,
                    allReverse);
        }

        // Upbounded
        final RangeRequest upbounded = RangeRequest.builder().endRowExclusive(row(2)).build();
        assertRangeSizeAndOrdering(keyValueService.getRange(TEST_TABLE, upbounded, TEST_TIMESTAMP + 1),
                2,
                upbounded);

        if (reverseSupported) {
            final RangeRequest upboundedReverse = RangeRequest.reverseBuilder().endRowExclusive(row(0)).build();
            assertRangeSizeAndOrdering(keyValueService.getRange(TEST_TABLE, upboundedReverse, TEST_TIMESTAMP + 1),
                    2,
                    upboundedReverse);
        }

        // Downbounded
        final RangeRequest downbounded = RangeRequest.builder().startRowInclusive(row(1)).build();
        assertRangeSizeAndOrdering(keyValueService.getRange(TEST_TABLE, downbounded, TEST_TIMESTAMP + 1),
                2,
                downbounded);

        if (reverseSupported) {
            final RangeRequest downboundedReverse = RangeRequest.reverseBuilder().startRowInclusive(row(1)).build();
            assertRangeSizeAndOrdering(keyValueService.getRange(TEST_TABLE, downboundedReverse, TEST_TIMESTAMP + 1),
                    2,
                    downboundedReverse);
        }

        // Both-bounded
        final RangeRequest bothbound = RangeRequest.builder().startRowInclusive(row(1)).endRowExclusive(row(2)).build();
        assertRangeSizeAndOrdering(keyValueService.getRange(TEST_TABLE, bothbound, TEST_TIMESTAMP + 1),
                1,
                bothbound);

        if (reverseSupported) {
            final RangeRequest bothboundedReverse = RangeRequest.reverseBuilder()
                    .startRowInclusive(row(2))
                    .endRowExclusive(row(1))
                    .build();
            assertRangeSizeAndOrdering(keyValueService.getRange(TEST_TABLE, bothboundedReverse, TEST_TIMESTAMP + 1),
                    1,
                    bothboundedReverse);
        }

        // Precise test for lower-bounded
        RangeRequest rangeRequest = downbounded;
        ClosableIterator<RowResult<Value>> rangeResult = keyValueService.getRange(
                TEST_TABLE,
                rangeRequest,
                TEST_TIMESTAMP + 1);
        assertFalse(keyValueService.getRange(TEST_TABLE, rangeRequest, TEST_TIMESTAMP).hasNext());
        assertTrue(rangeResult.hasNext());
        assertEquals(
                RowResult.create(
                        row(1),
                        ImmutableSortedMap.orderedBy(UnsignedBytes.lexicographicalComparator()).put(
                                column(0),
                                Value.create(val(1, 0), TEST_TIMESTAMP)).put(
                                column(2),
                                Value.create(val(1, 2), TEST_TIMESTAMP)).build()),
                rangeResult.next());
        assertTrue(rangeResult.hasNext());
        assertEquals(
                RowResult.create(
                        row(2),
                        ImmutableSortedMap.orderedBy(UnsignedBytes.lexicographicalComparator()).put(
                                column(1),
                                Value.create(val(2, 1), TEST_TIMESTAMP)).put(
                                column(2),
                                Value.create(val(2, 2), TEST_TIMESTAMP)).build()),
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
        values.put(Cell.create(PtBytes.toBytes("00"), PtBytes.toBytes("c1")),
                PtBytes.toBytes("a"));
        values.put(Cell.create(PtBytes.toBytes("00"), PtBytes.toBytes("c2")),
                PtBytes.toBytes("b"));

        values.put(Cell.create(PtBytes.toBytes("01"), RangeRequests.getFirstRowName()),
                PtBytes.toBytes("c"));

        values.put(Cell.create(PtBytes.toBytes("02"), PtBytes.toBytes("c1")),
                PtBytes.toBytes("d"));
        values.put(Cell.create(PtBytes.toBytes("02"), PtBytes.toBytes("c2")),
                PtBytes.toBytes("e"));

        values.put(Cell.create(PtBytes.toBytes("03"), PtBytes.toBytes("c1")),
                PtBytes.toBytes("f"));

        values.put(Cell.create(PtBytes.toBytes("04"), PtBytes.toBytes("c1")),
                PtBytes.toBytes("g"));
        values.put(Cell.create(PtBytes.toBytes("04"), RangeRequests.getLastRowName()),
                PtBytes.toBytes("h"));

        values.put(Cell.create(PtBytes.toBytes("05"), PtBytes.toBytes("c1")),
                PtBytes.toBytes("i"));
        values.put(Cell.create(RangeRequests.getLastRowName(), PtBytes.toBytes("c1")),
                PtBytes.toBytes("j"));
        keyValueService.put(tableRef, values, TEST_TIMESTAMP);

        RangeRequest request = RangeRequest.builder(reverse).batchHint(batchSizeHint).build();
        try (ClosableIterator<RowResult<Value>> iter = keyValueService.getRange(tableRef, request, Long.MAX_VALUE)) {
            List<RowResult<Value>> results = ImmutableList.copyOf(iter);
            List<RowResult<Value>> expected = ImmutableList.of(
                    RowResult.create(PtBytes.toBytes("00"),
                        ImmutableSortedMap.<byte[], Value>orderedBy(UnsignedBytes.lexicographicalComparator())
                            .put(PtBytes.toBytes("c1"),
                                    Value.create(PtBytes.toBytes("a"), TEST_TIMESTAMP))
                            .put(PtBytes.toBytes("c2"),
                                    Value.create(PtBytes.toBytes("b"), TEST_TIMESTAMP))
                            .build()),
                    RowResult.create(PtBytes.toBytes("01"),
                        ImmutableSortedMap.<byte[], Value>orderedBy(UnsignedBytes.lexicographicalComparator())
                            .put(RangeRequests.getFirstRowName(),
                                    Value.create(PtBytes.toBytes("c"), TEST_TIMESTAMP))
                            .build()),
                    RowResult.create(PtBytes.toBytes("02"),
                        ImmutableSortedMap.<byte[], Value>orderedBy(UnsignedBytes.lexicographicalComparator())
                            .put(PtBytes.toBytes("c1"),
                                    Value.create(PtBytes.toBytes("d"), TEST_TIMESTAMP))
                            .put(PtBytes.toBytes("c2"),
                                    Value.create(PtBytes.toBytes("e"), TEST_TIMESTAMP))
                            .build()),
                    RowResult.create(PtBytes.toBytes("03"),
                        ImmutableSortedMap.<byte[], Value>orderedBy(UnsignedBytes.lexicographicalComparator())
                            .put(PtBytes.toBytes("c1"),
                                    Value.create(PtBytes.toBytes("f"), TEST_TIMESTAMP))
                            .build()),
                    RowResult.create(PtBytes.toBytes("04"),
                        ImmutableSortedMap.<byte[], Value>orderedBy(UnsignedBytes.lexicographicalComparator())
                            .put(PtBytes.toBytes("c1"),
                                    Value.create(PtBytes.toBytes("g"), TEST_TIMESTAMP))
                            .put(RangeRequests.getLastRowName(),
                                    Value.create(PtBytes.toBytes("h"), TEST_TIMESTAMP))
                            .build()),
                    RowResult.create(PtBytes.toBytes("05"),
                        ImmutableSortedMap.<byte[], Value>orderedBy(UnsignedBytes.lexicographicalComparator())
                            .put(PtBytes.toBytes("c1"),
                                    Value.create(PtBytes.toBytes("i"), TEST_TIMESTAMP))
                            .build()),
                    RowResult.create(RangeRequests.getLastRowName(),
                        ImmutableSortedMap.<byte[], Value>orderedBy(UnsignedBytes.lexicographicalComparator())
                            .put(PtBytes.toBytes("c1"),
                                    Value.create(PtBytes.toBytes("j"), TEST_TIMESTAMP))
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
                Cell.create(last, PtBytes.toBytes("c1")), PtBytes.toBytes("a"),
                Cell.create(last, last), PtBytes.toBytes("b"));
        keyValueService.put(tableRef, values, TEST_TIMESTAMP);

        RangeRequest request = RangeRequest.builder(reverse).batchHint(batchSizeHint).build();
        try (ClosableIterator<RowResult<Value>> iter = keyValueService.getRange(tableRef, request, Long.MAX_VALUE)) {
            List<RowResult<Value>> results = ImmutableList.copyOf(iter);
            List<RowResult<Value>> expected = ImmutableList.of(
                    RowResult.create(last,
                        ImmutableSortedMap.<byte[], Value>orderedBy(UnsignedBytes.lexicographicalComparator())
                                .put(PtBytes.toBytes("c1"),
                                        Value.create(PtBytes.toBytes("a"), TEST_TIMESTAMP))
                                .put(last, Value.create(PtBytes.toBytes("b"), TEST_TIMESTAMP))
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
        Map<Cell, byte[]> expectedValues = new HashMap<>();
        for (long row = 1; row <= numRows; ++row) {
            for (long col = 1; col <= row; ++col) {
                byte[] rowName = PtBytes.toBytes(Long.MIN_VALUE ^ row);
                byte[] colName = PtBytes.toBytes(Long.MIN_VALUE ^ col);
                expectedValues.put(Cell.create(rowName, colName), PtBytes.toBytes(row + "," + col));
            }
        }
        Map<Cell, byte[]> unexpectedValues = Maps.transformValues(expectedValues, val -> PtBytes.toBytes("foo"));

        keyValueService.truncateTable(TEST_TABLE);
        keyValueService.put(TEST_TABLE, expectedValues, TEST_TIMESTAMP); // only these should be returned

        keyValueService.put(TEST_TABLE, unexpectedValues, TEST_TIMESTAMP - 10);
        keyValueService.put(TEST_TABLE, unexpectedValues, TEST_TIMESTAMP + 10);
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
        try (ClosableIterator<RowResult<Value>> iter = keyValueService.getRange(TEST_TABLE, request,
                TEST_TIMESTAMP + 1)) {
            List<RowResult<Value>> results = ImmutableList.copyOf(iter);
            assertEquals(getExpectedResultForRangePagingWithColumnSelectionTest(numRows, numColsInSelection, reverse),
                    results);
        }
    }

    private static List<RowResult<Value>> getExpectedResultForRangePagingWithColumnSelectionTest(
            int numRows,
            int numColsInSelection,
            boolean reverse) {
        List<RowResult<Value>> expected = new ArrayList<>();
        for (long row = 1; row <= numRows; ++row) {
            ImmutableSortedMap.Builder<byte[], Value> builder = ImmutableSortedMap.orderedBy(
                    UnsignedBytes.lexicographicalComparator());
            for (long col = 1; col <= row && col <= numColsInSelection; ++col) {
                byte[] colName = PtBytes.toBytes(Long.MIN_VALUE ^ col);
                builder.put(colName, Value.create(PtBytes.toBytes(row + "," + col), TEST_TIMESTAMP));
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
                ImmutableMultimap.of(Cell.create(row(0), column(1)), TEST_TIMESTAMP));
        assertEquals(2, Iterators.size(keyValueService.getRange(
                TEST_TABLE,
                RangeRequest.all(),
                TEST_TIMESTAMP + 1)));
        keyValueService.delete(
                TEST_TABLE,
                ImmutableMultimap.of(Cell.create(row(1), column(0)), TEST_TIMESTAMP));
        assertEquals(2, Iterators.size(keyValueService.getRange(
                TEST_TABLE,
                RangeRequest.all(),
                TEST_TIMESTAMP + 1)));
        keyValueService.delete(
                TEST_TABLE,
                ImmutableMultimap.of(Cell.create(row(1), column(2)), TEST_TIMESTAMP));
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
        assertFalse(result.hasNext());

        result = keyValueService.getRange(TEST_TABLE, RangeRequest.all(), TEST_TIMESTAMP + 2);
        assertTrue(result.hasNext());
    }

    @Test
    public void testDeleteRangeReverse() {
        Assume.assumeTrue(reverseRangesSupported());
        // should delete only row1
        setupTestRowsZeroOneAndTwoAndDeleteFrom(PtBytes.toBytes("row1b"), row(0), true);
        checkThatTableIsNowOnly(row(0), row(2));
    }

    @Test
    public void testDeleteRangeSingleRow() {
        // should delete row0 only
        setupTestRowsZeroOneAndTwoAndDeleteFrom(row(0), RangeRequests.nextLexicographicName(row(0)));
        checkThatTableIsNowOnly(row(1), row(2));
    }

    @Test
    public void testDeleteRangeStartRowInclusivity() {
        // should delete row0 and row1
        setupTestRowsZeroOneAndTwoAndDeleteFrom(row(0), PtBytes.toBytes("row1b"));
        checkThatTableIsNowOnly(row(2));
    }

    @Test
    public void testDeleteRangeEndRowExclusivity() {
        // should delete row0 only
        setupTestRowsZeroOneAndTwoAndDeleteFrom(PtBytes.toBytes("row"), row(1));
        checkThatTableIsNowOnly(row(1), row(2));
    }

    @Test
    public void testDeleteRangeAll() {
        putTestDataForRowsZeroOneAndTwo();
        keyValueService.deleteRange(TEST_TABLE, RangeRequest.all());
        checkThatTableIsNowOnly();
    }

    @Test
    public void testDeleteRangeNone() {
        setupTestRowsZeroOneAndTwoAndDeleteFrom(PtBytes.toBytes("a"),
                PtBytes.toBytes("a"));
        checkThatTableIsNowOnly(row(0), row(1), row(2));
    }

    @Test
    public void deleteRowsWithNothing() {
        setupTestRowsZeroOneAndTwoAndDeleteSpecific(ImmutableList.of());
        checkThatTableIsNowOnly(row(0), row(1), row(2));
    }

    @Test
    public void deleteRowsDeletesOneRow() {
        setupTestRowsZeroOneAndTwoAndDeleteSpecific(ImmutableList.of(row(0)));
        checkThatTableIsNowOnly(row(1), row(2));
    }

    @Test
    public void deleteRowsDeletesMultipleRows() {
        setupTestRowsZeroOneAndTwoAndDeleteSpecific(ImmutableList.of(row(0), row(2), row(1)));
        checkThatTableIsNowOnly();
    }

    @Test
    public void deleteRowsDeletesMultipleNoncontiguousRows() {
        setupTestRowsZeroOneAndTwoAndDeleteSpecific(ImmutableList.of(row(0), row(2)));
        checkThatTableIsNowOnly(row(1));
    }

    @Test
    public void deleteRowsIgnoresRowsThatDoNotExist() {
        setupTestRowsZeroOneAndTwoAndDeleteSpecific(ImmutableList.of(row(5), row(7)));
        checkThatTableIsNowOnly(row(0), row(1), row(2));
    }

    @Test
    public void deleteRowsResilientToDuplicates() {
        setupTestRowsZeroOneAndTwoAndDeleteSpecific(ImmutableList.of(row(0), row(0), row(0), row(0)));
        checkThatTableIsNowOnly(row(1), row(2));
    }

    @Test
    public void deleteTimestampRangesIgnoresEmptyMap() {
        keyValueService.deleteAllTimestamps(TEST_TABLE, ImmutableMap.of());
    }

    @Test
    public void deleteTimestampRangesDeletesForSingleColumn() {
        long ts1 = 5L;
        long ts2 = 10L;
        long latestTs = 15L;

        keyValueService.putWithTimestamps(TEST_TABLE, ImmutableMultimap.of(
                TEST_CELL, Value.create(val(0, 5), ts1),
                TEST_CELL, Value.create(val(0, 7), ts2),
                TEST_CELL, Value.create(val(0, 9), latestTs)));

        legacyDeleteAllTimestamps(TEST_TABLE, ImmutableMap.of(TEST_CELL, latestTs), false);

        assertThat(getAllTimestampsForTestCell(), contains(latestTs));
        assertThat(keyValueService.get(TEST_TABLE, ImmutableMap.of(TEST_CELL, Long.MAX_VALUE)).get(TEST_CELL),
                equalTo(Value.create(val(0, 9), latestTs)));
    }

    @Test
    public void deleteTimestampRangesDeletesMultipleColumnsAcrossMultipleRows() {
        Cell row0col0 = Cell.create(row(0), column(0));
        Cell row0col1 = Cell.create(row(0), column(1));
        Cell row1col0 = Cell.create(row(1), column(0));

        long ts1Col0 = 5L;
        long ts2Col0 = 10L;
        long latestTsCol0 = 15L;

        long ts1Col1 = 2L;
        long ts2Col1 = 3L;
        long latestTsCol1 = 4L;

        keyValueService.put(TEST_TABLE, ImmutableMap.of(row0col0, val(0, 0)), ts1Col0);
        keyValueService.put(TEST_TABLE, ImmutableMap.of(row0col0, val(0, 1)), ts2Col0);
        keyValueService.put(TEST_TABLE, ImmutableMap.of(row0col0, val(1, 0)), latestTsCol0);

        keyValueService.put(TEST_TABLE, ImmutableMap.of(row1col0, val(0, 0)), ts1Col0);
        keyValueService.put(TEST_TABLE, ImmutableMap.of(row1col0, val(0, 1)), ts2Col0);
        keyValueService.put(TEST_TABLE, ImmutableMap.of(row1col0, val(1, 0)), latestTsCol0);

        keyValueService.put(TEST_TABLE, ImmutableMap.of(row0col1, val(0, 0)), ts1Col1);
        keyValueService.put(TEST_TABLE, ImmutableMap.of(row0col1, val(0, 1)), ts2Col1);
        keyValueService.put(TEST_TABLE, ImmutableMap.of(row0col1, val(1, 0)), latestTsCol1);

        legacyDeleteAllTimestamps(TEST_TABLE,
                ImmutableMap.of(row0col0, latestTsCol0, row0col1, latestTsCol1, row1col0, latestTsCol0),
                false);

        assertThat(keyValueService.getAllTimestamps(TEST_TABLE, ImmutableSet.of(row0col0), Long.MAX_VALUE).asMap().get(
                row0col0), contains(latestTsCol0));
        assertThat(keyValueService.getAllTimestamps(TEST_TABLE, ImmutableSet.of(row0col1), Long.MAX_VALUE).asMap().get(
                row0col1), contains(latestTsCol1));
        assertThat(keyValueService.getAllTimestamps(TEST_TABLE, ImmutableSet.of(row1col0), Long.MAX_VALUE).asMap().get(
                row1col0), contains(latestTsCol0));

        assertThat(keyValueService.get(TEST_TABLE, ImmutableMap.of(row0col0, Long.MAX_VALUE)).get(row0col0),
                equalTo(Value.create(val(1, 0), latestTsCol0)));
        assertThat(keyValueService.get(TEST_TABLE, ImmutableMap.of(row0col1, Long.MAX_VALUE)).get(row0col1),
                equalTo(Value.create(val(1, 0), latestTsCol1)));
        assertThat(keyValueService.get(TEST_TABLE, ImmutableMap.of(row1col0, Long.MAX_VALUE)).get(row1col0),
                equalTo(Value.create(val(1, 0), latestTsCol0)));
    }

    @Test
    public void deleteTimestampRangesIncludingSentinelsIgnoresEmptyMap() {
        legacyDeleteAllTimestamps(TEST_TABLE, ImmutableMap.of(), true);
    }

    @Test
    public void deleteTimestampRangesIncludingSentinelsDeletesForSingleColumn() {
        long ts1 = 5L;
        long ts2 = 10L;
        long latestTs = 15L;

        keyValueService.putWithTimestamps(TEST_TABLE, ImmutableMultimap.of(
                TEST_CELL, Value.create(val(0, 5), ts1),
                TEST_CELL, Value.create(val(0, 7), ts2),
                TEST_CELL, Value.create(val(0, 9), latestTs)));

        legacyDeleteAllTimestamps(TEST_TABLE, ImmutableMap.of(TEST_CELL, latestTs), true);

        assertThat(getAllTimestampsForTestCell(), contains(latestTs));
        assertThat(keyValueService.get(TEST_TABLE, ImmutableMap.of(TEST_CELL, Long.MAX_VALUE)).get(TEST_CELL),
                equalTo(Value.create(val(0, 9), latestTs)));
    }

    @Test
    public void deleteTimestampRangesIncludingSentinelsDeletesMultipleColumnsAcrossMultipleRows() {
        Cell row0col0 = Cell.create(row(0), column(0));
        Cell row0col1 = Cell.create(row(0), column(1));
        Cell row1col0 = Cell.create(row(1), column(0));

        long ts1Col0 = 5L;
        long ts2Col0 = 10L;
        long latestTsCol0 = 15L;

        long ts1Col1 = 2L;
        long ts2Col1 = 3L;
        long latestTsCol1 = 4L;

        keyValueService.put(TEST_TABLE, ImmutableMap.of(row0col0, val(0, 0)), ts1Col0);
        keyValueService.put(TEST_TABLE, ImmutableMap.of(row0col0, val(0, 1)), ts2Col0);
        keyValueService.put(TEST_TABLE, ImmutableMap.of(row0col0, val(1, 0)), latestTsCol0);

        keyValueService.put(TEST_TABLE, ImmutableMap.of(row1col0, val(0, 0)), ts1Col0);
        keyValueService.put(TEST_TABLE, ImmutableMap.of(row1col0, val(0, 1)), ts2Col0);
        keyValueService.put(TEST_TABLE, ImmutableMap.of(row1col0, val(1, 0)), latestTsCol0);

        keyValueService.put(TEST_TABLE, ImmutableMap.of(row0col1, val(0, 0)), ts1Col1);
        keyValueService.put(TEST_TABLE, ImmutableMap.of(row0col1, val(0, 1)), ts2Col1);
        keyValueService.put(TEST_TABLE, ImmutableMap.of(row0col1, val(1, 0)), latestTsCol1);

        legacyDeleteAllTimestamps(TEST_TABLE, ImmutableMap.of(
                row0col0, latestTsCol0,
                row0col1, latestTsCol1,
                row1col0, latestTsCol0), true);

        assertThat(keyValueService.getAllTimestamps(TEST_TABLE, ImmutableSet.of(row0col0), Long.MAX_VALUE).asMap().get(
                row0col0), contains(latestTsCol0));
        assertThat(keyValueService.getAllTimestamps(TEST_TABLE, ImmutableSet.of(row0col1), Long.MAX_VALUE).asMap().get(
                row0col1), contains(latestTsCol1));
        assertThat(keyValueService.getAllTimestamps(TEST_TABLE, ImmutableSet.of(row1col0), Long.MAX_VALUE).asMap().get(
                row1col0), contains(latestTsCol0));

        assertThat(keyValueService.get(TEST_TABLE, ImmutableMap.of(row0col0, Long.MAX_VALUE)).get(row0col0),
                equalTo(Value.create(val(1, 0), latestTsCol0)));
        assertThat(keyValueService.get(TEST_TABLE, ImmutableMap.of(row0col1, Long.MAX_VALUE)).get(row0col1),
                equalTo(Value.create(val(1, 0), latestTsCol1)));
        assertThat(keyValueService.get(TEST_TABLE, ImmutableMap.of(row1col0, Long.MAX_VALUE)).get(row1col0),
                equalTo(Value.create(val(1, 0), latestTsCol0)));
    }

    @Test
    public void deleteTimestampRangesLeavesSentinels() {
        long latestTs = 15L;

        keyValueService.addGarbageCollectionSentinelValues(TEST_TABLE, ImmutableSet.of(TEST_CELL));
        keyValueService.put(TEST_TABLE, ImmutableMap.of(TEST_CELL, val(1, 0)), latestTs);

        legacyDeleteAllTimestamps(TEST_TABLE, ImmutableMap.of(TEST_CELL, latestTs), false);

        assertThat(getAllTimestampsForTestCell(), contains(Value.INVALID_VALUE_TIMESTAMP, latestTs));
        assertThat(keyValueService.get(TEST_TABLE, ImmutableMap.of(TEST_CELL, Value.INVALID_VALUE_TIMESTAMP + 1L))
                .get(TEST_CELL), equalTo(Value.create(new byte[0], Value.INVALID_VALUE_TIMESTAMP)));
    }

    @Test
    public void deleteTimestampRangesIncludingSentinelsDeletesSentinels() {
        long latestTs = 15L;

        keyValueService.addGarbageCollectionSentinelValues(TEST_TABLE, ImmutableSet.of(TEST_CELL));
        keyValueService.put(TEST_TABLE, ImmutableMap.of(TEST_CELL, val(1, 0)), latestTs);

        legacyDeleteAllTimestamps(TEST_TABLE, ImmutableMap.of(TEST_CELL, latestTs), true);

        assertThat(getAllTimestampsForTestCell(), contains(latestTs));
    }

    @Test
    public void deleteTimestampRangesEdgeCases() {
        long minTs = 0;

        keyValueService.addGarbageCollectionSentinelValues(TEST_TABLE, ImmutableSet.of(TEST_CELL));
        keyValueService.putWithTimestamps(TEST_TABLE, ImmutableMultimap.of(
                TEST_CELL, Value.create(val(0, 5), minTs),
                TEST_CELL, Value.create(val(0, 7), TEST_TIMESTAMP - 1),
                TEST_CELL, Value.create(val(0, 9), TEST_TIMESTAMP)));

        legacyDeleteAllTimestamps(TEST_TABLE, ImmutableMap.of(TEST_CELL, TEST_TIMESTAMP), false);

        assertThat(getAllTimestampsForTestCell(), contains(Value.INVALID_VALUE_TIMESTAMP, TEST_TIMESTAMP));
    }

    @Test
    public void deleteTimestampRangesIncludingSentinelsEdgeCases() {
        long minTs = 0;

        keyValueService.addGarbageCollectionSentinelValues(TEST_TABLE, ImmutableSet.of(TEST_CELL));
        keyValueService.putWithTimestamps(TEST_TABLE, ImmutableMultimap.of(
                TEST_CELL, Value.create(val(0, 5), minTs),
                TEST_CELL, Value.create(val(0, 7), TEST_TIMESTAMP - 1),
                TEST_CELL, Value.create(val(0, 9), TEST_TIMESTAMP)));

        legacyDeleteAllTimestamps(TEST_TABLE, ImmutableMap.of(TEST_CELL, TEST_TIMESTAMP), true);

        assertThat(getAllTimestampsForTestCell(), contains(TEST_TIMESTAMP));
    }

    private Collection<Long> getAllTimestampsForTestCell() {
        return keyValueService.getAllTimestamps(TEST_TABLE, ImmutableSet.of(TEST_CELL), Long.MAX_VALUE)
                .asMap().get(TEST_CELL);
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

    private void setupTestRowsZeroOneAndTwoAndDeleteSpecific(List<byte[]> rows) {
        putTestDataForRowsZeroOneAndTwo();

        keyValueService.deleteRows(TEST_TABLE, rows);
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
        final Value val1 = Value.create(val(0, 7), TEST_TIMESTAMP + 1);
        final Value val5 = Value.create(val(0, 9), TEST_TIMESTAMP + 5);
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
            range = RangeRequest.builder().startRowInclusive(row(0)).endRowExclusive(row(1)).build();
        } else {
            range = RangeRequest.reverseBuilder().startRowInclusive(row(0)).build();
        }
        ClosableIterator<RowResult<Set<Long>>> rangeWithHistory = keyValueService.getRangeOfTimestamps(
                TEST_TABLE, range, TEST_TIMESTAMP + 2);

        RowResult<Set<Long>> row = rangeWithHistory.next();
        assertFalse(rangeWithHistory.hasNext());
        rangeWithHistory.close();
        assertEquals(1, Iterables.size(row.getCells()));
        Entry<Cell, Set<Long>> cell0 = row.getCells().iterator().next();
        assertEquals(2, cell0.getValue().size());
        assertTrue(cell0.getValue().contains(TEST_TIMESTAMP));
        assertTrue(cell0.getValue().contains(TEST_TIMESTAMP + 1));
    }

    @Test
    public void testGetRangeOfTimestampsReturnsAllRows() {
        keyValueService.put(TEST_TABLE,
                ImmutableMap.of(
                    Cell.create(row(0), column(0)), val(0, 5),
                    Cell.create(row(1), column(0)), val(0, 5),
                    Cell.create(row(2), column(0)), val(0, 5)),
                TEST_TIMESTAMP);
        RangeRequest range = RangeRequest.all().withBatchHint(1);
        List<RowResult<Set<Long>>> results = ImmutableList.copyOf(
                keyValueService.getRangeOfTimestamps(TEST_TABLE, range, TEST_TIMESTAMP + 1));
        assertEquals(3, results.size());
        assertArrayEquals(row(0), results.get(0).getRowName());
        assertArrayEquals(row(1), results.get(1).getRowName());
        assertArrayEquals(row(2), results.get(2).getRowName());
    }

    @Test
    public void testGetRangeOfTimestampsOmitsTimestampsLessThanMax() {
        keyValueService.put(TEST_TABLE,
                ImmutableMap.of(
                        Cell.create(row(0), column(0)), val(0, 5)),
                TEST_TIMESTAMP);

        keyValueService.put(TEST_TABLE,
                ImmutableMap.of(
                        Cell.create(row(0), column(0)), val(0, 7)),
                TEST_TIMESTAMP + 10);

        RangeRequest range = RangeRequest.all().withBatchHint(2);
        List<RowResult<Set<Long>>> results = ImmutableList.copyOf(
                keyValueService.getRangeOfTimestamps(TEST_TABLE, range, TEST_TIMESTAMP + 1));
        assertEquals(1, results.size());
        assertArrayEquals(row(0), results.get(0).getRowName());
        assertEquals(TEST_TIMESTAMP, (long) results.get(0).getOnlyColumnValue().iterator().next());
    }

    @Test
    public void testGetRangeOfTimestampsFetchesProperRange() {
        keyValueService.put(TEST_TABLE,
                ImmutableMap.of(
                        Cell.create(row(0), column(0)), val(0, 5),
                        Cell.create(row(1), column(0)), val(0, 5),
                        Cell.create(row(2), column(0)), val(0, 5)),
                TEST_TIMESTAMP);

        keyValueService.put(TEST_TABLE,
                ImmutableMap.of(
                        Cell.create(row(0), column(0)), val(0, 7)),
                TEST_TIMESTAMP + 10);

        RangeRequest range = RangeRequest.builder().startRowInclusive(row(1)).endRowExclusive(row(2)).build();
        List<RowResult<Set<Long>>> results = ImmutableList.copyOf(
                keyValueService.getRangeOfTimestamps(TEST_TABLE, range, TEST_TIMESTAMP + 1));
        assertEquals(1, results.size());
        assertArrayEquals(row(1), results.get(0).getRowName());
    }

    @Test
    public void testKeyAlreadyExists() {
        // Test that it does not throw some random exceptions
        putTestDataForSingleTimestamp();
        try {
            putTestDataForSingleTimestamp();
        } catch (AtlasDbDependencyException e) {
            if (e.getCause() instanceof KeyAlreadyExistsException) {
                Assert.fail("Must not throw when overwriting with same value!");
            }
        }

        keyValueService.putWithTimestamps(
                TEST_TABLE,
                ImmutableMultimap.of(
                        TEST_CELL,
                        Value.create(val(0, 0), TEST_TIMESTAMP + 1)));
        try {
            keyValueService.putWithTimestamps(
                    TEST_TABLE,
                    ImmutableMultimap.of(
                            TEST_CELL,
                            Value.create(val(0, 0), TEST_TIMESTAMP + 1)));
        } catch (AtlasDbDependencyException e) {
            if (e.getCause() instanceof KeyAlreadyExistsException) {
                Assert.fail("Must not throw when overwriting with same value!");
            }
        }

        try {
            keyValueService.putWithTimestamps(TEST_TABLE, ImmutableMultimap.of(
                    TEST_CELL, Value.create(val(0, 1), TEST_TIMESTAMP + 1)));
            // Legal
        } catch (KeyAlreadyExistsException e) {
            // Legal
        }

        // The first try might not throw as putUnlessExists must only be exclusive with other putUnlessExists.
        try {
            keyValueService.putUnlessExists(TEST_TABLE, ImmutableMap.of(TEST_CELL, val(0, 0)));
            // Legal
        } catch (KeyAlreadyExistsException e) {
            // Legal
        }

        assertThatThrownBy(() -> keyValueService.putUnlessExists(TEST_TABLE, ImmutableMap.of(TEST_CELL, val(0, 0))))
                .as("putUnlessExists must throw when overwriting the same cell!")
                .isInstanceOf(KeyAlreadyExistsException.class);
    }

    @Test
    public void putUnlessExistsDoesNotConflictForMultipleCellsSameRow() {
        Assume.assumeTrue(checkAndSetSupported());
        Cell firstTestCell = Cell.create(row(0), column(0));
        Cell nextTestCell = Cell.create(row(0), column(1));

        keyValueService.putUnlessExists(TEST_TABLE, ImmutableMap.of(firstTestCell, val(0, 0)));
        keyValueService.putUnlessExists(TEST_TABLE, ImmutableMap.of(nextTestCell, val(0, 1)));
        // Legal as the cells are different
    }

    @Test
    public void putUnlessExistsDoesNotConflictForMultipleCellsSameColumn() {
        Assume.assumeTrue(checkAndSetSupported());
        Cell firstTestCell = Cell.create(row(0), column(0));
        Cell nextTestCell = Cell.create(row(1), column(0));

        keyValueService.putUnlessExists(TEST_TABLE, ImmutableMap.of(firstTestCell, val(0, 0)));
        keyValueService.putUnlessExists(TEST_TABLE, ImmutableMap.of(nextTestCell, val(0, 1)));
        // Legal as the cells are different
    }

    @Test
    public void canPutUnlessExistsMultipleValuesInSameRow() {
        Assume.assumeTrue(checkAndSetSupported());
        Cell firstTestCell = Cell.create(row(0), column(0));
        Cell nextTestCell = Cell.create(row(0), column(1));

        keyValueService.putUnlessExists(TEST_TABLE, ImmutableMap.of(firstTestCell, val(0, 0), nextTestCell, val(0, 1)));

        Map<Cell, Value> storedValues = keyValueService.get(TEST_TABLE,
                ImmutableMap.of(firstTestCell, Long.MAX_VALUE, nextTestCell, Long.MAX_VALUE));
        assertArrayEquals(val(0, 0), storedValues.get(firstTestCell).getContents());
        assertArrayEquals(val(0, 1), storedValues.get(nextTestCell).getContents());
    }

    @Test
    public void putUnlessExistsConflictsOnAnyColumnMismatch() {
        Assume.assumeTrue(checkAndSetSupported());
        Cell firstTestCell = Cell.create(row(0), column(0));
        Cell nextTestCell = Cell.create(row(0), column(1));

        keyValueService.putUnlessExists(TEST_TABLE, ImmutableMap.of(firstTestCell, val(0, 0)));

        // Exact message is KVS specific so not asserting on that
        assertThatThrownBy(() -> keyValueService.putUnlessExists(TEST_TABLE,
                        ImmutableMap.of(firstTestCell, val(0, 0), nextTestCell, val(0, 1))))
                .isInstanceOf(KeyAlreadyExistsException.class);
    }

    @Test
    public void putUnlessExistsLargeValue() {
        Assume.assumeTrue(checkAndSetSupported());
        byte[] megabyteValue = new byte[1048576];

        keyValueService.putUnlessExists(TEST_TABLE, ImmutableMap.of(TEST_CELL, megabyteValue));

        Value storedValue = keyValueService.get(TEST_TABLE, ImmutableMap.of(TEST_CELL, Long.MAX_VALUE)).get(TEST_CELL);
        assertArrayEquals(megabyteValue, storedValue.getContents());
    }

    @Test
    public void putUnlessExistsDecodesCellsCorrectlyIfSupported() {
        Assume.assumeTrue(keyValueService.getCheckAndSetCompatibility()
                == CheckAndSetCompatibility.SUPPORTED_DETAIL_ON_FAILURE);

        keyValueService.putUnlessExists(TEST_TABLE, ImmutableMap.of(TEST_CELL, val(0, 0)));

        // Exact message is KVS specific so not asserting on that
        assertThatThrownBy(() -> keyValueService.putUnlessExists(TEST_TABLE, ImmutableMap.of(TEST_CELL, val(0, 0))))
                .isInstanceOf(KeyAlreadyExistsException.class)
                .satisfies(exception -> {
                    KeyAlreadyExistsException keyAlreadyExistsException = (KeyAlreadyExistsException) exception;
                    assertThat(keyAlreadyExistsException.getExistingKeys(), contains(TEST_CELL));
                });
    }

    @Test
    public void testCheckAndSetFromEmpty() {
        Assume.assumeTrue(checkAndSetSupported());

        CheckAndSetRequest request = CheckAndSetRequest.newCell(TEST_TABLE, TEST_CELL, val(0, 0));
        keyValueService.checkAndSet(request);

        verifyCheckAndSet(TEST_CELL, val(0, 0));
    }

    @Test
    public void testCheckAndSetFromOtherValue() {
        Assume.assumeTrue(checkAndSetSupported());

        CheckAndSetRequest request = CheckAndSetRequest.newCell(TEST_TABLE, TEST_CELL, val(0, 0));
        keyValueService.checkAndSet(request);

        CheckAndSetRequest secondRequest = CheckAndSetRequest.singleCell(TEST_TABLE, TEST_CELL, val(0, 0), val(0, 1));
        keyValueService.checkAndSet(secondRequest);

        verifyCheckAndSet(TEST_CELL, val(0, 1));
    }

    @Test
    public void testCheckAndSetAndBackAgain() {
        testCheckAndSetFromOtherValue();

        CheckAndSetRequest thirdRequest = CheckAndSetRequest.singleCell(TEST_TABLE, TEST_CELL, val(0, 1), val(0, 0));
        keyValueService.checkAndSet(thirdRequest);

        verifyCheckAndSet(TEST_CELL, val(0, 0));
    }

    @Test
    public void testCheckAndSetLargeValue() {
        Assume.assumeTrue(checkAndSetSupported());
        byte[] megabyteValue = new byte[1048576];
        CheckAndSetRequest request = CheckAndSetRequest.newCell(TEST_TABLE, TEST_CELL, megabyteValue);

        keyValueService.checkAndSet(request);
        verifyCheckAndSet(TEST_CELL, megabyteValue);
    }

    private void verifyCheckAndSet(Cell key, byte[] expectedValue) {
        Multimap<Cell, Long> timestamps = keyValueService.getAllTimestamps(TEST_TABLE, ImmutableSet.of(key), 1L);

        assertEquals(1, timestamps.size());
        assertTrue(timestamps.containsEntry(key, AtlasDbConstants.TRANSACTION_TS));

        ClosableIterator<RowResult<Value>> result = keyValueService.getRange(TEST_TABLE, RangeRequest.all(),
                AtlasDbConstants.TRANSACTION_TS + 1);

        // Check result is right
        byte[] actual = result.next().getColumns().get(key.getColumnName()).getContents();
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

        CheckAndSetRequest request = CheckAndSetRequest.newCell(TEST_TABLE, TEST_CELL, val(0, 0));
        keyValueService.checkAndSet(request);

        try {
            CheckAndSetRequest secondRequest = CheckAndSetRequest
                    .singleCell(TEST_TABLE, TEST_CELL, val(0, 1), val(0, 0));
            keyValueService.checkAndSet(secondRequest);
        } catch (CheckAndSetException ex) {
            assertThat(ex.getActualValues(), contains(val(0, 0)));
            throw ex;
        }
    }

    @Test(expected = CheckAndSetException.class)
    public void testCheckAndSetFromValueWhenNoValue() {
        Assume.assumeTrue(checkAndSetSupported());

        CheckAndSetRequest request = CheckAndSetRequest.singleCell(TEST_TABLE, TEST_CELL, val(0, 0), val(0, 1));
        keyValueService.checkAndSet(request);
    }

    @Test(expected = CheckAndSetException.class)
    public void testCheckAndSetFromNoValueWhenValueIsPresent() {
        Assume.assumeTrue(checkAndSetSupported());

        CheckAndSetRequest request = CheckAndSetRequest.newCell(TEST_TABLE, TEST_CELL, val(0, 0));
        keyValueService.checkAndSet(request);
        keyValueService.checkAndSet(request);
    }

    @Test
    public void testCheckAndSetToNewCellsInDistinctRows() {
        Assume.assumeTrue(checkAndSetSupported());
        Cell firstTestCell = Cell.create(row(0), column(0));
        Cell nextTestCell = Cell.create(row(0), column(1));

        keyValueService.checkAndSet(CheckAndSetRequest.newCell(TEST_TABLE, firstTestCell, val(0, 0)));
        keyValueService.checkAndSet(CheckAndSetRequest.newCell(TEST_TABLE, nextTestCell, val(0, 1)));

        verifyCheckAndSet(firstTestCell, val(0, 0));
        verifyCheckAndSet(nextTestCell, val(0, 1));
    }

    @Test
    public void testCheckAndSetIndependentlyWorks() {
        Assume.assumeTrue(checkAndSetSupported());
        Cell firstTestCell = Cell.create(row(0), column(0));
        Cell nextTestCell = Cell.create(row(0), column(1));

        keyValueService.checkAndSet(CheckAndSetRequest.newCell(TEST_TABLE, firstTestCell, val(0, 0)));
        keyValueService.checkAndSet(CheckAndSetRequest.newCell(TEST_TABLE, nextTestCell, val(0, 1)));
        keyValueService.checkAndSet(CheckAndSetRequest.singleCell(TEST_TABLE, firstTestCell, val(0, 0), val(0, 1)));

        verifyCheckAndSet(firstTestCell, val(0, 1));
        verifyCheckAndSet(nextTestCell, val(0, 1));
    }

    @Test
    public void testCheckAndSetIndependentlyFails() {
        Assume.assumeTrue(checkAndSetSupported());
        Cell firstTestCell = Cell.create(row(0), column(0));
        Cell nextTestCell = Cell.create(row(0), column(1));

        keyValueService.checkAndSet(CheckAndSetRequest.newCell(TEST_TABLE, firstTestCell, val(0, 0)));
        keyValueService.checkAndSet(CheckAndSetRequest.newCell(TEST_TABLE, nextTestCell, val(0, 1)));

        assertThatThrownBy(() -> keyValueService.checkAndSet(
                CheckAndSetRequest.singleCell(TEST_TABLE, nextTestCell, val(0, 0), val(0, 1))))
                .isInstanceOf(CheckAndSetException.class);

        verifyCheckAndSet(firstTestCell, val(0, 0));
        verifyCheckAndSet(nextTestCell, val(0, 1));
    }

    @Test
    public void testAddGcSentinelValues() {
        putTestDataForMultipleTimestamps();

        Multimap<Cell, Long> timestampsBefore = getTestTimestamps();
        assertEquals(2, timestampsBefore.size());
        assertFalse(timestampsBefore.containsEntry(TEST_CELL, Value.INVALID_VALUE_TIMESTAMP));

        keyValueService.addGarbageCollectionSentinelValues(TEST_TABLE, ImmutableSet.of(TEST_CELL));

        Multimap<Cell, Long> timestampsAfter1 = getTestTimestamps();
        assertEquals(3, timestampsAfter1.size());
        assertTrue(timestampsAfter1.containsEntry(TEST_CELL, Value.INVALID_VALUE_TIMESTAMP));

        keyValueService.addGarbageCollectionSentinelValues(TEST_TABLE, ImmutableSet.of(TEST_CELL));

        Multimap<Cell, Long> timestampsAfter2 = getTestTimestamps();
        assertEquals(3, timestampsAfter2.size());
        assertTrue(timestampsAfter2.containsEntry(TEST_CELL, Value.INVALID_VALUE_TIMESTAMP));
    }

    private Multimap<Cell, Long> getTestTimestamps() {
        return keyValueService.getAllTimestamps(TEST_TABLE, ImmutableSet.of(TEST_CELL), AtlasDbConstants.MAX_TS);
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

    @Test(expected = RuntimeException.class)
    public void testGetRangeOfTimestampsThrowsOnError() {
        keyValueService.getRangeOfTimestamps(TEST_NONEXISTING_TABLE, RangeRequest.all(), AtlasDbConstants.MAX_TS)
                .hasNext();
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

    private static void modifyValue(byte[] retrievedValue) {
        retrievedValue[0] = (byte) 50;
    }

    private static byte[] copyOf(byte[] contents) {
        return Arrays.copyOf(contents, contents.length);
    }

    private void writeToCell(Cell cell, byte[] data) {
        Value val = Value.create(data, TEST_TIMESTAMP + 1);
        keyValueService.putWithTimestamps(TEST_TABLE, ImmutableMultimap.of(cell, val));
    }

    private byte[] getRowsForCell(Cell cell) {
        return keyValueService.getRows(TEST_TABLE, ImmutableSet.of(cell.getRowName()),
                ColumnSelection.all(),
                TEST_TIMESTAMP + 3)
                .get(cell).getContents();
    }

    private byte[] getForCell(Cell cell) {
        return keyValueService.get(TEST_TABLE, ImmutableMap.of(cell, TEST_TIMESTAMP + 3)).get(cell).getContents();
    }

    private byte[] getOnlyItemInTableRange() {
        try (ClosableIterator<RowResult<Value>> rangeIterator =
                     keyValueService.getRange(TEST_TABLE, RangeRequest.all(), TEST_TIMESTAMP + 3)) {
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

        assertThat(keyValueService.get(DynamicColumnTable.reference(), valueToGet), is(Collections.emptyMap()));
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

        assertThat(values, is(Collections.emptyMap()));
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

    @Test
    public void clusterAvailabilityStatusShouldBeAllAvailable() {
        assertThat(keyValueService.getClusterAvailabilityStatus(), is(ClusterAvailabilityStatus.ALL_AVAILABLE));
    }

    private static byte[] row(int number) {
        return PtBytes.toBytes("row" + number);
    }

    private static byte[] column(int number) {
        return PtBytes.toBytes("column" + number);
    }

    private static byte[] val(int row, int col) {
        return PtBytes.toBytes("value" + row + col);
    }

    private static byte[] dynamicColumn(long columnId) {
        return PtBytes.toBytes(columnId);
    }

    protected void putTestDataForRowsZeroOneAndTwo() {
        keyValueService.put(TEST_TABLE,
                ImmutableMap.of(Cell.create(row(0), column(0)), val(0, 5)), TEST_TIMESTAMP);
        keyValueService.put(TEST_TABLE,
                ImmutableMap.of(Cell.create(row(1), column(0)), val(0, 5)), TEST_TIMESTAMP);
        keyValueService.put(TEST_TABLE,
                ImmutableMap.of(Cell.create(row(2), column(0)), val(0, 5)), TEST_TIMESTAMP);
    }

    protected void putTestDataForMultipleTimestamps() {
        keyValueService.put(TEST_TABLE,
                ImmutableMap.of(TEST_CELL, val(0, 5)), TEST_TIMESTAMP);
        keyValueService.put(TEST_TABLE,
                ImmutableMap.of(TEST_CELL, val(0, 7)), TEST_TIMESTAMP + 1);
    }

    protected void putTestDataForSingleTimestamp() {
        /*      | column0     column1     column2
         * -----+---------------------------------
         * row0 | value00     value01     -
         * row1 | value10     -           value12
         * row2 | -           value21     value22
         */
        Map<Cell, byte[]> values = new HashMap<>();
        values.put(TEST_CELL, val(0, 0));
        values.put(Cell.create(row(0), column(1)), val(0, 1));
        values.put(Cell.create(row(1), column(0)), val(1, 0));
        values.put(Cell.create(row(1), column(2)), val(1, 2));
        values.put(Cell.create(row(2), column(1)), val(2, 1));
        values.put(Cell.create(row(2), column(2)), val(2, 2));
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
        keyValueService.createTable(tableRef, TableMetadata.builder()
                .columns(new ColumnMetadataDescription(columns))
                .nameLogSafety(LogSafety.SAFE)
                .build()
                .persistToBytes());
        keyValueService.truncateTable(tableRef);
        return tableRef;
    }

    private void legacyDeleteAllTimestamps(
            TableReference tableRef, Map<Cell, Long> cellToMaxTimestamp, boolean deleteSentinels) {
        Map<Cell, TimestampRangeDelete> deletes = Maps.transformValues(cellToMaxTimestamp,
                timestamp -> new TimestampRangeDelete.Builder()
                        .timestamp(timestamp)
                        .endInclusive(false) // Used to always be exclusive
                        .deleteSentinels(deleteSentinels)
                        .build());
        keyValueService.deleteAllTimestamps(tableRef, deletes);
    }
}
