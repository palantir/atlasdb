// Copyright 2015 Palantir Technologies
//
// Licensed under the BSD-3 License (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://opensource.org/licenses/BSD-3-Clause
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.atlasdb.keyvalue.impl;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Value;

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
                ColumnSelection.create(ImmutableList.of(cell1.getColumnName(), cell3.getColumnName())),
                1);
        assertEquals(ImmutableSet.of(cell1, cell3), rows3.keySet());

        Map<Cell, Value> rows4 = keyValueService.getRows(
                TEST_TABLE,
                ImmutableSet.of(cell1.getRowName()),
                ColumnSelection.create(ImmutableList.<byte[]>of()),
                1);
        assertEquals(ImmutableSet.of(), rows4.keySet());
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
                ImmutableMap.of(Cell.create(row0, column0), TEST_TIMESTAMP + 2));
        assertTrue("Incorrect number of values returned.", timestamps.size() == 1);
        assertEquals("Incorrect value returned.", new Long(TEST_TIMESTAMP + 1),
                timestamps.get(Cell.create(row0, column0)));
    }

    @Test
    public void testGetWithMultipleVersions() {
        putTestDataForMultipleTimestamps();
        Map<Cell, Value> values = keyValueService.get(TEST_TABLE,
                ImmutableMap.of(Cell.create(row0, column0), TEST_TIMESTAMP + 2));
        assertTrue("Incorrect number of values returned.", values.size() == 1);
        assertEquals("Incorrect value returned.", Value.create(value0_t1, TEST_TIMESTAMP + 1),
                values.get(Cell.create(row0, column0)));
    }

    private void putTestDataForMultipleTimestamps() {
        keyValueService.put(TEST_TABLE,
                ImmutableMap.of(Cell.create(row0, column0), value0_t0), TEST_TIMESTAMP);
        keyValueService.put(TEST_TABLE,
                ImmutableMap.of(Cell.create(row0, column0), value0_t1), TEST_TIMESTAMP + 1);
    }

    private void putTestDataForSingleTimestamp() {
        /*      | column0     column1     column2
         * -----+---------------------------------
         * row0 | "value00"   "value01"   -
         * row1 | "value10"   -           "value12"
         * row2 | -           "value21"   "value22"
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

        for (long timestamp: timestamps) {
            keyValueService.put(TEST_TABLE, ImmutableMap.of(cell,
                    PtBytes.toBytes("val" + timestamp)), timestamp);
        }
    }

    protected abstract KeyValueService getKeyValueService();
}
