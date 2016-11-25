/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.cassandra.multinode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.common.base.ClosableIterator;


public class OneNodeDownGetTest {

    @Test
    public void canGet() {
        verifyTimestampAndValue(OneNodeDownTestSuite.CELL_1_1, OneNodeDownTestSuite.DEFAULT_TIMESTAMP,
                OneNodeDownTestSuite.DEFAULT_VALUE);
    }

    @Test
    public void canGetRows() {
        Map<Cell, Value> row = OneNodeDownTestSuite.db.getRows(OneNodeDownTestSuite.TEST_TABLE,
                ImmutableList.of(OneNodeDownTestSuite.FIRST_ROW),
                ColumnSelection.all(), Long.MAX_VALUE);

        assertEquals(OneNodeDownTestSuite.DEFAULT_TIMESTAMP, row.get(OneNodeDownTestSuite.CELL_1_1).getTimestamp());
        assertEquals(new String(OneNodeDownTestSuite.DEFAULT_VALUE),
                new String(row.get(OneNodeDownTestSuite.CELL_1_1).getContents()));
        assertEquals(OneNodeDownTestSuite.DEFAULT_TIMESTAMP, row.get(OneNodeDownTestSuite.CELL_1_2).getTimestamp());
        assertEquals(new String(OneNodeDownTestSuite.DEFAULT_VALUE),
                new String(row.get(OneNodeDownTestSuite.CELL_1_2).getContents()));
    }

    @Test
    public void canGetRange() {
        final RangeRequest range = RangeRequest.builder().endRowExclusive(OneNodeDownTestSuite.SECOND_ROW).build();
        ClosableIterator<RowResult<Value>> it = OneNodeDownTestSuite.db.getRange(OneNodeDownTestSuite.TEST_TABLE, range,
                Long.MAX_VALUE);

        RowResult<Value> row = it.next();
        assertEquals(new String(OneNodeDownTestSuite.FIRST_ROW), new String(row.getRowName()));
        assertEquals(2, row.getColumns().size());
        assertFalse(it.hasNext());
    }

    @Test
    public void canGetRowsColumnRange() {
        BatchColumnRangeSelection rangeSelection = BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY,
                PtBytes.EMPTY_BYTE_ARRAY, 1);
        Map<byte[], RowColumnRangeIterator> rowsColumnRange = OneNodeDownTestSuite.db.getRowsColumnRange(
                OneNodeDownTestSuite.TEST_TABLE,
                ImmutableList.of(OneNodeDownTestSuite.FIRST_ROW), rangeSelection, Long.MAX_VALUE);

        assertEquals(1, rowsColumnRange.size());
        byte[] rowName = rowsColumnRange.keySet().iterator().next();
        assertEquals(new String(OneNodeDownTestSuite.FIRST_ROW), new String(rowName));

        RowColumnRangeIterator it = rowsColumnRange.get(rowName);

        Map.Entry<Cell, Value> column1 = it.next();
        assertEquals(new String(OneNodeDownTestSuite.DEFAULT_VALUE), new String(column1.getValue().getContents()));
        Map.Entry<Cell, Value> column2 = it.next();
        assertEquals(new String(OneNodeDownTestSuite.DEFAULT_VALUE), new String(column2.getValue().getContents()));

        assertFalse(it.hasNext());
    }

    @Test
    public void canGetAllTableNames() {
        Iterator<TableReference> it = OneNodeDownTestSuite.db.getAllTableNames().iterator();
        assertEquals(OneNodeDownTestSuite.TEST_TABLE, it.next());
        assertFalse(it.hasNext());
    }

    @Test
    public void canGetLatestTimestamps() {
        Map<Cell, Long> latest = OneNodeDownTestSuite.db.getLatestTimestamps(OneNodeDownTestSuite.TEST_TABLE,
                ImmutableMap.of(OneNodeDownTestSuite.CELL_1_1, Long.MAX_VALUE));
        assertEquals(OneNodeDownTestSuite.DEFAULT_TIMESTAMP, latest.get(OneNodeDownTestSuite.CELL_1_1).longValue());
    }

    @Test
    public void canGetRangeOfTimestamps() {
        RangeRequest range = RangeRequest.builder().endRowExclusive(OneNodeDownTestSuite.SECOND_ROW).build();
        ClosableIterator<RowResult<Set<Long>>> it = OneNodeDownTestSuite.db.getRangeOfTimestamps(
                OneNodeDownTestSuite.TEST_TABLE, range, Long.MAX_VALUE);

        RowResult<Set<Long>> rowResult = it.next();
        assertEquals(new String(OneNodeDownTestSuite.FIRST_ROW), new String(rowResult.getRowName()));

        SortedMap<byte[], Set<Long>> timestampMap = rowResult.getColumns();
        assertEquals(2, timestampMap.get(OneNodeDownTestSuite.FIRST_COLUMN).size());
        assertEquals(1, timestampMap.get(OneNodeDownTestSuite.SECOND_COLUMN).size());

        assertFalse(it.hasNext());
    }


    @Test
    public void canGetAllTimestamps() {
        Multimap<Cell, Long> result = OneNodeDownTestSuite.db.getAllTimestamps(OneNodeDownTestSuite.TEST_TABLE,
                ImmutableSet.of(OneNodeDownTestSuite.CELL_1_1), Long.MAX_VALUE);
        assertEquals(2, result.get(OneNodeDownTestSuite.CELL_1_1).size());
    }

    protected static void verifyTimestampAndValue(Cell cell, long timestamp, byte[] value) {
        Map<Cell, Value> result = OneNodeDownTestSuite.db.get(OneNodeDownTestSuite.TEST_TABLE,
                ImmutableMap.of(cell, Long.MAX_VALUE));
        assertEquals(timestamp, result.get(cell).getTimestamp());
        assertEquals(new String(value), new String(result.get(cell).getContents()));
    }

}
