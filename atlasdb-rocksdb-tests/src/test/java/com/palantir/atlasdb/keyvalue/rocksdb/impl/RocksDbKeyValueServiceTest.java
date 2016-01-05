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
package com.palantir.atlasdb.keyvalue.rocksdb.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.common.base.ClosableIterator;

public final class RocksDbKeyValueServiceTest {
    private static final byte[] COMMIT_TS_COLUMN = PtBytes.toBytes("t");
    private static final String TRANSACTION_TABLE = "_transactions";
    private RocksDbKeyValueService db = null;


    @Before
    public void setUp() throws Exception {
        db = RocksDbKeyValueService.create("testdb");
        for (String table : db.getAllTableNames()) {
            if (!table.equals("default") && !table.equals("_metadata")) {
                db.dropTable(table);
            }
        }
        db.createTable("yo", AtlasDbConstants.EMPTY_TABLE_METADATA);
    }


    @After
    public void tearDown() throws Exception {
        if (db != null) {
            db.close();
            db = null;
        }
    }


    @Test
    public void testCreate() {
        db.createTable("yo", AtlasDbConstants.EMPTY_TABLE_METADATA);
        db.createTable("yodog", AtlasDbConstants.EMPTY_TABLE_METADATA);
        db.createTable(TRANSACTION_TABLE, AtlasDbConstants.EMPTY_TABLE_METADATA);
        assertEquals(ImmutableSet.of("yo", "yodog", TRANSACTION_TABLE),
                     db.getAllTableNames());
    }


    @Test
    public void testReadNoExist() {
        final Cell cell = Cell.create("r1".getBytes(), COMMIT_TS_COLUMN);
        final Map<Cell, Value> res = db.get("yo", ImmutableMap.of(cell, 1L));
        assertTrue(res.isEmpty());
    }


    @Test
    public void testReadGood() {
        final Cell cell = Cell.create("r1".getBytes(), "2".getBytes());
        db.put("yo", ImmutableMap.of(cell, "v1".getBytes()), 1);
        final Map<Cell, Value> res = db.get("yo", ImmutableMap.of(cell, 2L));
        assertEquals(1, res.size());
        final Value value = res.get(cell);
        assertEquals(1, value.getTimestamp());
        assertEquals("v1", new String(value.getContents()));
    }


    @Test
    public void testReadGood2() {
        final Cell cell = Cell.create("r1".getBytes(), "2".getBytes());
        final Cell cell2 = Cell.create("r".getBytes(), "12".getBytes());
        db.put("yo", ImmutableMap.of(cell, "v1".getBytes()), 1000);
        db.put("yo", ImmutableMap.of(cell2, "v2".getBytes()), 1000);
        final Map<Cell, Value> res = db.get("yo", ImmutableMap.of(cell, 1001L));
        final Value value = res.get(cell);
        assertEquals(1000, value.getTimestamp());
        assertEquals("v1", new String(value.getContents()));
    }


    @Test
    public void testReadGood3() {
        final Cell cell = Cell.create("r1".getBytes(), COMMIT_TS_COLUMN);
        db.put("yo", ImmutableMap.of(cell, "v1".getBytes()), Long.MAX_VALUE - 3);
        final Map<Cell, Value> res = db.get("yo", ImmutableMap.of(cell, Long.MAX_VALUE - 2));
        final Value value = res.get(cell);
        assertEquals(Long.MAX_VALUE-3, value.getTimestamp());
        assertEquals("v1", new String(value.getContents()));
    }


    @Test
    public void testReadGood4() {
        final Cell cell = Cell.create("r,1".getBytes(), ",c,1,".getBytes());
        db.put("yo", ImmutableMap.of(cell, "v,1".getBytes()), 1);
        final Map<Cell, Value> res = db.get("yo", ImmutableMap.of(cell, 2L));
        final Value value = res.get(cell);
        assertEquals(1, value.getTimestamp());
        assertEquals("v,1", new String(value.getContents()));
    }


    @Test
    public void testReadBeforeTime() {
        final Cell cell = Cell.create("r1".getBytes(), COMMIT_TS_COLUMN);
        db.put("yo", ImmutableMap.of(cell, "v1".getBytes()), 2);
        final Map<Cell, Value> res = db.get("yo", ImmutableMap.of(cell, 2L));
        assertTrue(res.isEmpty());
    }


    @Test
    public void testGetRow() {
        final Cell cell = Cell.create("r1".getBytes(), "c1".getBytes());
        final Cell cell2 = Cell.create("r1".getBytes(), "c2".getBytes());
        db.put("yo", ImmutableMap.of(cell, "v1".getBytes()), 2);
        db.put("yo", ImmutableMap.of(cell2, "v2".getBytes()), 2);
        final Map<Cell, Value> rows = db.getRows("yo", ImmutableList.of("r1".getBytes()), ColumnSelection.all(), 3);
        assertEquals(2, rows.size());
    }


    @Test
    public void testGetRange() {
        final Cell cell = Cell.create("r1".getBytes(), "c1".getBytes());
        final Cell cell2 = Cell.create("r1".getBytes(), "c2".getBytes());
        final Cell cell3 = Cell.create("r2".getBytes(), "c2".getBytes());
        db.put("yo", ImmutableMap.of(cell, "v1".getBytes()), 2);
        db.put("yo", ImmutableMap.of(cell2, "v2".getBytes()), 2);
        db.put("yo", ImmutableMap.of(cell3, "v3".getBytes()), 4);
        final RangeRequest range = RangeRequest.builder().endRowExclusive("r2".getBytes()).build();
        final ClosableIterator<? extends RowResult<Value>> it = db.getRange("yo", range, 10);
        try {
            final List<RowResult<Value>> list = Lists.newArrayList();
            Iterators.addAll(list, it);
            assertEquals(1, list.size());
            final Map<Cell, Value> rows = db.getRows("yo", ImmutableList.of("r1".getBytes()), ColumnSelection.all(), 3);
            assertEquals(2, rows.size());
            final RowResult<Value> row = list.iterator().next();
            final Map<Cell, Value> cellsFromRow = putAll(Maps.<Cell, Value>newHashMap(), row.getCells());
            assertEquals(rows, cellsFromRow);
        } finally {
            it.close();
        }
    }


    @Test
    public void testGetRange2() {
        final Cell cell = Cell.create(",r,1".getBytes(), ",c,1,".getBytes());
        db.put("yo", ImmutableMap.of(cell, "v1".getBytes()), 2);
        final RangeRequest range = RangeRequest.builder().build();
        final ClosableIterator<RowResult<Value>> it = db.getRange("yo", range, 10);
        try {
            final List<RowResult<Value>> list = Lists.newArrayList();
            Iterators.addAll(list, it);
            assertEquals(1, list.size());
            final RowResult<Value> row = list.iterator().next();
            final Map<Cell, Value> cellsFromRow = putAll(Maps.<Cell, Value>newHashMap(), row.getCells());
            final Map<Cell, Value> rows = db.getRows("yo", ImmutableList.of(",r,1".getBytes()), ColumnSelection.all(), 3);
            assertEquals(rows, cellsFromRow);
        } finally {
            it.close();
        }
    }


    @Test
    public void testGetRowCellOverlap() {
        final Cell cell = Cell.create("12".getBytes(), "34".getBytes());
        final Cell cell2 = Cell.create("1".getBytes(), "23".getBytes());
        db.put("yo", ImmutableMap.of(cell, "v1".getBytes()), 2);
        db.put("yo", ImmutableMap.of(cell2, "v2".getBytes()), 2);
        final Map<Cell, Value> rows = db.getRows("yo", ImmutableList.of("12".getBytes()), ColumnSelection.all(), 3);
        assertEquals(1, rows.size());
    }


    @Test
    public void testGetRangeCellOverlap() {
        final Cell cell = Cell.create("12".getBytes(), "34".getBytes());
        final Cell cell2 = Cell.create("1".getBytes(), "235".getBytes());
        db.put("yo", ImmutableMap.of(cell, "v1".getBytes()), 2);
        db.put("yo", ImmutableMap.of(cell2, "v2".getBytes()), 2);
        ClosableIterator<? extends RowResult<Value>> it = db.getRange("yo", RangeRequest.builder().build(), 3);
        try {
            assertEquals(2, Iterators.size(it));
        } finally {
            it.close();
        }
        it = db.getRange("yo", RangeRequest.builder().endRowExclusive("12".getBytes()).build(), 3);
        try {
            assertEquals(1, Iterators.size(it));
        } finally {
            it.close();
        }
        it = db.getRange("yo", RangeRequest.builder().startRowInclusive("12".getBytes()).build(), 3);
        try {
            assertEquals(1, Iterators.size(it));
        } finally {
            it.close();
        }
    }

    @Test
    public void testGetRangeCellOverlap2() {
        final Cell cell = Cell.create("1".getBytes(), "1".getBytes());
        final Cell cell2 = Cell.create("12".getBytes(), "0".getBytes());
        final Cell cell3 = Cell.create("1".getBytes(), "3".getBytes());
        db.put("yo", ImmutableMap.of(cell, "v1".getBytes()), 2);
        db.put("yo", ImmutableMap.of(cell2, "v2".getBytes()), 2);
        db.put("yo", ImmutableMap.of(cell3, "v3".getBytes()), 2);
        final ClosableIterator<? extends RowResult<Value>> it = db.getRange("yo", RangeRequest.builder().build(), 3);
        try {
            assertEquals(2, Iterators.size(it));
        } finally {
            it.close();
        }
    }


    @Test
    public void testDoubleWriteToTransactionTable() {
        db.createTable(TRANSACTION_TABLE, AtlasDbConstants.EMPTY_TABLE_METADATA);
        final Cell cell = Cell.create("r1".getBytes(), COMMIT_TS_COLUMN);
        db.putUnlessExists(TRANSACTION_TABLE, ImmutableMap.of(cell, "v1".getBytes()));
        try {
            db.putUnlessExists(TRANSACTION_TABLE, ImmutableMap.of(cell, "v2".getBytes()));
            fail();
        } catch (KeyAlreadyExistsException e) {
            // expected
        }
        final Map<Cell, Value> res = db.get(TRANSACTION_TABLE, ImmutableMap.of(cell, 1L));
        final Value value = res.get(cell);
        assertEquals(0L, value.getTimestamp());
        assertEquals("v1", new String(value.getContents()));
    }


    @Test
    public void testDoubleOpen() {
        try {
            RocksDbKeyValueService.create("testdb");
            fail("should have thrown");
        } catch (Exception e) {
            //expected
        }
    }


    @Test
    public void testMetadata() {
        db.putMetadataForTable("yo", "yoyo".getBytes());
        final byte[] meta = db.getMetadataForTable("yo");
        assertEquals("yoyo", new String (meta));
    }


    @Test
    public void testCreateTables() {
        db.putMetadataForTable("yo", "yoyo".getBytes());
        final byte[] meta = db.getMetadataForTable("yo");
        assertEquals("yoyo", new String (meta));
    }


    private static <K, V> Map<K, V> putAll(Map<K, V> map, Iterable<? extends Map.Entry<? extends K, ? extends V>> it) {
        for (Map.Entry<? extends K, ? extends V> e : it) {
            map.put(e.getKey(), e.getValue());
        }
        return map;
    }
}
