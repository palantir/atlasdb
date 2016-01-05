/**
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.ArrayUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Multimap;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.common.base.ClosableIterator;

public abstract class AbstractAtlasDbKeyValueServiceTest {
    protected static final String TEST_TABLE = "ns.pt_kvs_test";
    protected static final String TEST_NONEXISTING_TABLE = "ns2.some_nonexisting_table";

    protected static final byte[] row0 = "row0".getBytes();
    protected static final byte[] row1 = "row1".getBytes();
    protected static final byte[] row2 = "row2".getBytes();
    protected static final byte[] column0 = "column0".getBytes();
    protected static final byte[] column1 = "column1".getBytes();
    protected static final byte[] column2 = "column2".getBytes();
    protected static final byte[] value00 = "value00".getBytes();
    protected static final byte[] value01 = "value01".getBytes();
    protected static final byte[] value10 = "value10".getBytes();
    protected static final byte[] value12 = "value12".getBytes();
    protected static final byte[] value21 = "value21".getBytes();
    protected static final byte[] value22 = "value22".getBytes();

    protected static final byte[] value0_t0 = "value0_t0".getBytes();
    protected static final byte[] value0_t1 = "value1_t1".getBytes();
    protected static final byte[] value0_t5 = "value5_t5".getBytes();

    protected static final byte[] metadata0 = "metadata0".getBytes();

    protected static final long TEST_TIMESTAMP = 1000000l;

    protected KeyValueService keyValueService;

    protected boolean reverseRangesSupported() {
        return true;
    }

    @Before
    public void setUp() throws Exception {
        keyValueService = getKeyValueService();
        keyValueService.createTable(TEST_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);
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
    public void testGetWhenMultipleVersions() {
        putTestDataForMultipleTimestamps();
        Cell cell = Cell.create(row0, column0);
        Value val0 = Value.create(value0_t0, TEST_TIMESTAMP);
        Value val1 = Value.create(value0_t1, TEST_TIMESTAMP + 1);

        assertTrue(keyValueService.get(TEST_TABLE, ImmutableMap.of(cell, TEST_TIMESTAMP)).isEmpty());

        Map<Cell, Value> result = keyValueService.get(
                TEST_TABLE,
                ImmutableMap.of(cell, TEST_TIMESTAMP + 1));
        assertTrue(result.containsKey(cell));
        assertEquals(1, result.size());
        assertTrue(result.containsValue(val0));

        result = keyValueService.get(TEST_TABLE, ImmutableMap.of(cell, TEST_TIMESTAMP + 2));

        assertEquals(1, result.size());
        assertTrue(result.containsKey(cell));
        assertTrue(result.containsValue(val1));

        result = keyValueService.get(TEST_TABLE, ImmutableMap.of(cell, TEST_TIMESTAMP + 3));

        assertEquals(1, result.size());
        assertTrue(result.containsKey(cell));
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

    @Test
    public void testGetAllTableNames() {
        final String anotherTable = "AnotherTable";
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
        keyValueService.putMetadataForTable(TEST_TABLE, metadata0);
        assertTrue(Arrays.equals(metadata0, keyValueService.getMetadataForTable(TEST_TABLE)));
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
    public void testGetAllTimestamps() {
        putTestDataForMultipleTimestamps();
        final Cell cell = Cell.create(row0, column0);
        final Set<Cell> cellSet = ImmutableSet.of(cell);
        Multimap<Cell, Long> timestamps = keyValueService.getAllTimestamps(
                TEST_TABLE,
                cellSet,
                TEST_TIMESTAMP);
        assertEquals(0, timestamps.size());

        timestamps = keyValueService.getAllTimestamps(TEST_TABLE, cellSet, TEST_TIMESTAMP + 1);
        assertEquals(1, timestamps.size());
        assertTrue(timestamps.containsEntry(cell, TEST_TIMESTAMP));

        timestamps = keyValueService.getAllTimestamps(TEST_TABLE, cellSet, TEST_TIMESTAMP + 2);
        assertEquals(2, timestamps.size());
        assertTrue(timestamps.containsEntry(cell, TEST_TIMESTAMP));
        assertTrue(timestamps.containsEntry(cell, TEST_TIMESTAMP + 1));

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
                ImmutableMultimap.of(Cell.create(row0, column0), TEST_TIMESTAMP));
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
        Cell cell = Cell.create(row0, column0);
        ClosableIterator<RowResult<Value>> result = keyValueService.getRange(
                TEST_TABLE,
                RangeRequest.all(),
                TEST_TIMESTAMP + 1);
        assertTrue(result.hasNext());

        keyValueService.delete(TEST_TABLE, ImmutableMultimap.of(cell, TEST_TIMESTAMP));

        result = keyValueService.getRange(TEST_TABLE, RangeRequest.all(), TEST_TIMESTAMP + 1);
        assertTrue(!result.hasNext());

        result = keyValueService.getRange(TEST_TABLE, RangeRequest.all(), TEST_TIMESTAMP + 2);
        assertTrue(result.hasNext());
    }

    @Test
    public void testPutWithTimestamps() {
        putTestDataForMultipleTimestamps();
        final Cell cell = Cell.create(row0, column0);
        final Value val1 = Value.create(value0_t1, TEST_TIMESTAMP + 1);
        final Value val5 = Value.create(value0_t5, TEST_TIMESTAMP + 5);
        keyValueService.putWithTimestamps(TEST_TABLE, ImmutableMultimap.of(cell, val5));
        assertEquals(
                val5,
                keyValueService.get(TEST_TABLE, ImmutableMap.of(cell, TEST_TIMESTAMP + 6)).get(cell));
        assertEquals(
                val1,
                keyValueService.get(TEST_TABLE, ImmutableMap.of(cell, TEST_TIMESTAMP + 5)).get(cell));
        keyValueService.delete(TEST_TABLE, ImmutableMultimap.of(cell, TEST_TIMESTAMP + 5));
    }

    @Test
    public void testGetRangeWithHistory() {
        testGetRangeWithHistory(false);
        if (reverseRangesSupported()) {
            testGetRangeWithHistory(true);
        }
    }

    public void testGetRangeWithHistory(boolean reverse) {
        putTestDataForMultipleTimestamps();
        final RangeRequest range;
        if (!reverse) {
            range = RangeRequest.builder().startRowInclusive(row0).endRowExclusive(row1).build();
        } else {
            range = RangeRequest.reverseBuilder().startRowInclusive(row0).build();
        }
        ClosableIterator<RowResult<Set<Value>>> rangeWithHistory = keyValueService.getRangeWithHistory(
                TEST_TABLE, range, TEST_TIMESTAMP + 2);
        RowResult<Set<Value>> row0 = rangeWithHistory.next();
        assertTrue(!rangeWithHistory.hasNext());
        rangeWithHistory.close();
        assertEquals(1, Iterables.size(row0.getCells()));
        Entry<Cell, Set<Value>> cell0 = row0.getCells().iterator().next();
        assertEquals(2, cell0.getValue().size());
        assertTrue(cell0.getValue().contains(Value.create(value0_t0, TEST_TIMESTAMP)));
        assertTrue(cell0.getValue().contains(Value.create(value0_t1, TEST_TIMESTAMP + 1)));
    }

    @Test
    public void testGetRangeWithTimestamps() {
        testGetRangeWithTimestamps(false);
        if (reverseRangesSupported()) {
            testGetRangeWithTimestamps(true);
        }
    }

    public void testGetRangeWithTimestamps(boolean reverse) {
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
                        Cell.create(row0, column0),
                        Value.create(value00, TEST_TIMESTAMP + 1)));
        try {
            keyValueService.putWithTimestamps(
                    TEST_TABLE,
                    ImmutableMultimap.of(
                            Cell.create(row0, column0),
                            Value.create(value00, TEST_TIMESTAMP + 1)));
            // Legal
        } catch (KeyAlreadyExistsException e) {
            Assert.fail("Must not throw when overwriting with same value!");
        }

        try {
            keyValueService.putWithTimestamps(TEST_TABLE, ImmutableMultimap.of(Cell.create(row0, column0), Value.create(value01, TEST_TIMESTAMP + 1)));
            // Legal
        } catch (KeyAlreadyExistsException e) {
            // Legal
        }

        // The first try might not throw as putUnlessExists must only be exclusive with other putUnlessExists.
        try {
            keyValueService.putUnlessExists(TEST_TABLE, ImmutableMap.of(Cell.create(row0, column0), value00));
            // Legal
        } catch (KeyAlreadyExistsException e) {
            // Legal
        }

        try {
            keyValueService.putUnlessExists(TEST_TABLE, ImmutableMap.of(Cell.create(row0, column0), value00));
            Assert.fail("putUnlessExists must throw when overwriting the same cell!");
        } catch (KeyAlreadyExistsException e) {
            // Legal
        }
    }

    @Test
    public void testAddGCSentinelValues() {
        putTestDataForMultipleTimestamps();
        Cell cell = Cell.create(row0, column0);

        Multimap<Cell, Long> timestampsBefore = keyValueService.getAllTimestamps(TEST_TABLE, ImmutableSet.of(cell), Long.MAX_VALUE);
        assertEquals(2, timestampsBefore.size());
        assertTrue(!timestampsBefore.containsEntry(cell, Value.INVALID_VALUE_TIMESTAMP));

        keyValueService.addGarbageCollectionSentinelValues(TEST_TABLE, ImmutableSet.of(cell));

        Multimap<Cell, Long> timestampsAfter1 = keyValueService.getAllTimestamps(TEST_TABLE, ImmutableSet.of(cell), Long.MAX_VALUE);
        assertEquals(3, timestampsAfter1.size());
        assertTrue(timestampsAfter1.containsEntry(cell, Value.INVALID_VALUE_TIMESTAMP));

        keyValueService.addGarbageCollectionSentinelValues(TEST_TABLE, ImmutableSet.of(cell));

        Multimap<Cell, Long> timestampsAfter2 = keyValueService.getAllTimestamps(TEST_TABLE, ImmutableSet.of(cell), Long.MAX_VALUE);
        assertEquals(3, timestampsAfter2.size());
        assertTrue(timestampsAfter2.containsEntry(cell, Value.INVALID_VALUE_TIMESTAMP));
    }

    @Test
    public void testGetRangeThrowsOnError() {
        try {
            keyValueService.getRange(TEST_NONEXISTING_TABLE, RangeRequest.all(), Long.MAX_VALUE).hasNext();
            Assert.fail("getRange must throw on failure");
        } catch (RuntimeException e) {
            // Expected
        }
    }

    @Test
    public void testGetRangeWithHistoryThrowsOnError() {
        try {
            keyValueService.getRangeWithHistory(TEST_NONEXISTING_TABLE, RangeRequest.all(), Long.MAX_VALUE).hasNext();
            Assert.fail("getRangeWithHistory must throw on failure");
        } catch (RuntimeException e) {
            // Expected
        }
    }

    @Test
    public void testGetRangeOfTimestampsThrowsOnError() {
        try {
            keyValueService.getRangeOfTimestamps(TEST_NONEXISTING_TABLE, RangeRequest.all(), Long.MAX_VALUE).hasNext();
            Assert.fail("getRangeOfTimestamps must throw on failure");
        } catch (RuntimeException e) {
            // Expected
        }
    }

    protected void putTestDataForMultipleTimestamps() {
        keyValueService.put(TEST_TABLE,
                ImmutableMap.of(Cell.create(row0, column0), value0_t0), TEST_TIMESTAMP);
        keyValueService.put(TEST_TABLE,
                ImmutableMap.of(Cell.create(row0, column0), value0_t1), TEST_TIMESTAMP + 1);
    }

    protected void putTestDataForSingleTimestamp() {
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
