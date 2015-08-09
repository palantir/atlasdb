/**
 * // Copyright 2015 Palantir Technologies
 * //
 * // Licensed under the BSD-3 License (the "License");
 * // you may not use this file except in compliance with the License.
 * // You may obtain a copy of the License at
 * //
 * // http://opensource.org/licenses/BSD-3-Clause
 * //
 * // Unless required by applicable law or agreed to in writing, software
 * // distributed under the License is distributed on an "AS IS" BASIS,
 * // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * // See the License for the specific language governing permissions and
 * // limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.impl;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Multimap;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.common.base.ClosableIterator;

public abstract class AbstractAtlasDbKeyValueServiceTest {
    static final String TEST_TABLE = "pt_kvs_test";

    private static final byte[] row0 = "row0".getBytes();
    private static final byte[] row1 = "row1".getBytes();
    private static final byte[] row2 = "row2".getBytes();
    private static final byte[] column0 = "column0".getBytes();
    private static final byte[] column1 = "column1".getBytes();
    private static final byte[] column2 = "column2".getBytes();
    private static final byte[] value00 = "value00".getBytes();
    private static final byte[] value01 = "value01".getBytes();
    private static final byte[] value10 = "value10".getBytes();
    private static final byte[] value12 = "value12".getBytes();
    private static final byte[] value21 = "value21".getBytes();
    private static final byte[] value22 = "value22".getBytes();

    private static final byte[] value0_t0 = "value0_t0".getBytes();
    private static final byte[] value0_t1 = "value1_t1".getBytes();

    private static final byte[] metadata0 = "metadata0".getBytes();

    private static final long TEST_TIMESTAMP = 1000000l;

    KeyValueService keyValueService;

    @Before
    public void setUp() throws Exception {
        keyValueService = getKeyValueService();
        keyValueService.createTable(TEST_TABLE, Integer.MAX_VALUE);
    }

    @After
    public void tearDown() throws Exception {
        keyValueService.dropTable(TEST_TABLE);
        keyValueService.teardown();
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
                ColumnSelection.create(ImmutableList.of(
                        cell1.getColumnName(),
                        cell3.getColumnName())),
                1);
        assertEquals(ImmutableSet.of(cell1, cell3), rows3.keySet());

        Map<Cell, Value> rows4 = keyValueService.getRows(
                TEST_TABLE,
                ImmutableSet.of(cell1.getRowName()),
                ColumnSelection.create(ImmutableList.<byte[]> of()),
                1);
        assertEquals(ImmutableSet.of(), rows4.keySet());
    }

    @Test
    public void testGetRowsAllColumns() {
        putTestDataForSingleTimestamp();
        Map<Cell, Value> values = keyValueService.getRows(
                TEST_TABLE,
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

    @Test
    public void testGetRowsWhenMultipleVersions() {
        putTestDataForMultipleTimestamps();
        Map<Cell, Value> result = keyValueService.getRows(
                TEST_TABLE,
                ImmutableSet.of(row0),
                ColumnSelection.all(),
                TEST_TIMESTAMP + 1);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(Cell.create(row0, column0)));
        assertTrue(result.containsValue(Value.create(value0_t0, TEST_TIMESTAMP)));

        result = keyValueService.getRows(
                TEST_TABLE,
                ImmutableSet.of(row0),
                ColumnSelection.all(),
                TEST_TIMESTAMP + 2);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(Cell.create(row0, column0)));
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
        assertTrue(result.containsKey(Cell.create(row0, column0)));
        assertTrue(result.containsValue(Value.create(value0_t0, TEST_TIMESTAMP)));

        result = keyValueService.getRows(
                TEST_TABLE,
                ImmutableSet.of(row0),
                ColumnSelection.create(ImmutableSet.of(column0)),
                TEST_TIMESTAMP + 2);
        assertEquals(1, result.size());
        assertTrue(result.containsKey(Cell.create(row0, column0)));
        assertTrue(result.containsValue(Value.create(value0_t1, TEST_TIMESTAMP + 1)));

    }

    @Test
    public void testGetRowsWithSelectedColumns() {
        putTestDataForSingleTimestamp();
        ColumnSelection columns1and2 = ColumnSelection.create(Arrays.asList(column1, column2));
        Map<Cell, Value> values = keyValueService.getRows(
                TEST_TABLE,
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
        Map<Cell, Long> timestamps = keyValueService.getLatestTimestamps(
                TEST_TABLE,
                ImmutableMap.of(Cell.create(row0, column0), TEST_TIMESTAMP + 2));
        assertTrue("Incorrect number of values returned.", timestamps.size() == 1);
        assertEquals(
                "Incorrect value returned.",
                new Long(TEST_TIMESTAMP + 1),
                timestamps.get(Cell.create(row0, column0)));
    }

    @Test
    public void testGetWithMultipleVersions() {
        putTestDataForMultipleTimestamps();
        Map<Cell, Value> values = keyValueService.get(
                TEST_TABLE,
                ImmutableMap.of(Cell.create(row0, column0), TEST_TIMESTAMP + 2));
        assertTrue("Incorrect number of values returned.", values.size() == 1);
        assertEquals(
                "Incorrect value returned.",
                Value.create(value0_t1, TEST_TIMESTAMP + 1),
                values.get(Cell.create(row0, column0)));
    }

    @Test
    public void testGetAllTableNames() {
        final String anotherTable = "AnotherTable";
        assertEquals(1, keyValueService.getAllTableNames().size());
        assertEquals(TEST_TABLE, keyValueService.getAllTableNames().iterator().next());
        keyValueService.createTable(anotherTable, 123);
        assertEquals(2, keyValueService.getAllTableNames().size());
        assertTrue(keyValueService.getAllTableNames().contains(anotherTable));
        assertTrue(keyValueService.getAllTableNames().contains(TEST_TABLE));
        keyValueService.dropTable(anotherTable);
        assertEquals(1, keyValueService.getAllTableNames().size());
        assertEquals(TEST_TABLE, keyValueService.getAllTableNames().iterator().next());
    }

    @Test
    public void testTableMetadata() {
        assertEquals(0, keyValueService.getMetadataForTable(TEST_TABLE).length);
        keyValueService.putMetadataForTable(TEST_TABLE, metadata0);
        assertTrue(Arrays.equals(metadata0, keyValueService.getMetadataForTable(TEST_TABLE)));
    }

    @Test
    public void testGetRange() {
        putTestDataForSingleTimestamp();
        RangeRequest rangeRequest = RangeRequest.builder().startRowInclusive(row1).build();
        ClosableIterator<RowResult<Value>> rangeResult = keyValueService.getRange(
                TEST_TABLE,
                rangeRequest,
                TEST_TIMESTAMP + 1);
        assertTrue(keyValueService.getRange(TEST_TABLE, rangeRequest, TEST_TIMESTAMP).hasNext() == false);
        assertTrue(rangeResult.hasNext());
        assertEquals(
                RowResult.create(row1,
                        ImmutableSortedMap.orderedBy(UnsignedBytes.lexicographicalComparator())
                        .put(column0, Value.create(value10, TEST_TIMESTAMP))
                        .put(column2, Value.create(value12, TEST_TIMESTAMP)).build()),
                rangeResult.next());
        assertTrue(rangeResult.hasNext());
        assertEquals(
                RowResult.create(row2,
                        ImmutableSortedMap.orderedBy(UnsignedBytes.lexicographicalComparator())
                        .put(column1, Value.create(value21, TEST_TIMESTAMP))
                        .put(column2, Value.create(value22, TEST_TIMESTAMP)).build()),
                rangeResult.next());
        rangeResult.close();
    }

    @Test
    public void testGetAllTimestamps() {
        putTestDataForMultipleTimestamps();
        final Cell cell = Cell.create(row0, column0);
        final Set<Cell> cellSet = ImmutableSet.of(cell);
        Multimap<Cell, Long> timestamps = keyValueService.getAllTimestamps(
                TEST_TABLE,
                cellSet,
                TEST_TIMESTAMP);
        assertEquals(0, timestamps.size());

        timestamps = keyValueService.getAllTimestamps(
                TEST_TABLE,
                cellSet,
                TEST_TIMESTAMP + 1);
        assertEquals(1, timestamps.size());
        assertTrue(timestamps.containsEntry(cell, TEST_TIMESTAMP));

        timestamps = keyValueService.getAllTimestamps(
                TEST_TABLE,
                cellSet,
                TEST_TIMESTAMP + 2);
        assertEquals(2, timestamps.size());
        assertTrue(timestamps.containsEntry(cell, TEST_TIMESTAMP));
        assertTrue(timestamps.containsEntry(cell, TEST_TIMESTAMP + 1));

        assertEquals(
                timestamps,
                keyValueService.getAllTimestamps(
                        TEST_TABLE,
                        cellSet,
                        TEST_TIMESTAMP + 3));
    }

    private void putTestDataForMultipleTimestamps() {
        keyValueService.put(
                TEST_TABLE,
                ImmutableMap.of(Cell.create(row0, column0), value0_t0),
                TEST_TIMESTAMP);
        keyValueService.put(
                TEST_TABLE,
                ImmutableMap.of(Cell.create(row0, column0), value0_t1),
                TEST_TIMESTAMP + 1);
    }

    private void putTestDataForSingleTimestamp() {
        /*
         *      | column0    column1     column2
         * -----+---------------------------------
         * row0 | "value00"  "value01" -
         * row1 | "value10"  -           "value12"
         * row2 | -          "value21"   "value22"
         */
        Map<Cell, byte[]> values = new HashMap<Cell, byte[]>();
        values.put(Cell.create(row0, column0), value00);
        values.put(Cell.create(row0, column1), value01);
        values.put(Cell.create(row1, column0), value10);
        values.put(Cell.create(row1, column2), value12);
        values.put(Cell.create(row2, column1), value21);
        values.put(Cell.create(row2, column2), value22);
        keyValueService.put(TEST_TABLE, values, TEST_TIMESTAMP);
    }

    private void putValuesForTimestamps(Iterable<Long> timestamps) {
        Cell cell = Cell.create(PtBytes.toBytes("row"), PtBytes.toBytes("col"));

        for (long timestamp : timestamps) {
            keyValueService.put(
                    TEST_TABLE,
                    ImmutableMap.of(cell, PtBytes.toBytes("val" + timestamp)),
                    timestamp);
        }
    }

    protected abstract KeyValueService getKeyValueService();
}
