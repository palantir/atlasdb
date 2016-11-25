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

import static com.palantir.cassandra.multinode.OneNodeDownTestSuite.CELL_1_1;
import static com.palantir.cassandra.multinode.OneNodeDownTestSuite.CELL_1_2;
import static com.palantir.cassandra.multinode.OneNodeDownTestSuite.DEFAULT_TIMESTAMP;
import static com.palantir.cassandra.multinode.OneNodeDownTestSuite.DEFAULT_VALUE;
import static com.palantir.cassandra.multinode.OneNodeDownTestSuite.FIRST_ROW;
import static com.palantir.cassandra.multinode.OneNodeDownTestSuite.SECOND_ROW;
import static com.palantir.cassandra.multinode.OneNodeDownTestSuite.TEST_TABLE;
import static com.palantir.cassandra.multinode.OneNodeDownTestSuite.db;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.exception.PalantirRuntimeException;




public class OneNodeDownGetTest {

    @Rule
    public ExpectedException expect_exception = ExpectedException.none();

    @Test
    public void canGet() {
        verifyTimestampAndValue(CELL_1_1, DEFAULT_TIMESTAMP, DEFAULT_VALUE);
    }

    @Test
    public void canGetRows() {
        Map<Cell, Value> row = db.getRows(TEST_TABLE, ImmutableList.of(FIRST_ROW),
                ColumnSelection.all(), Long.MAX_VALUE);
        assertEquals(DEFAULT_TIMESTAMP, row.get(CELL_1_1).getTimestamp());
        assertEquals(new String(DEFAULT_VALUE), new String(row.get(CELL_1_1).getContents()));
        assertEquals(DEFAULT_TIMESTAMP, row.get(CELL_1_2).getTimestamp());
        assertEquals(new String(DEFAULT_VALUE), new String(row.get(CELL_1_2).getContents()));
    }

    @Test
    public void canGetRange() {
        final RangeRequest range = RangeRequest.builder().endRowExclusive(SECOND_ROW).build();
        ClosableIterator<RowResult<Value>> it = db.getRange(TEST_TABLE, range, Long.MAX_VALUE);

        RowResult<Value> row = it.next();
        assertEquals(false, it.hasNext());
        assertEquals(new String(FIRST_ROW), new String(row.getRowName()));
        assertEquals(2, row.getColumns().size());
    }

    @Test
    public void canGetRowsColumnRange() {
        BatchColumnRangeSelection rangeSelection = BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY,
                PtBytes.EMPTY_BYTE_ARRAY, 1);
        Map<byte[], RowColumnRangeIterator> rowsColumnRange = db.getRowsColumnRange(TEST_TABLE,
                ImmutableList.of(FIRST_ROW), rangeSelection, Long.MAX_VALUE);

        assertEquals(1, rowsColumnRange.size());

        byte[] rowName = rowsColumnRange.keySet().iterator().next();
        assertEquals(new String(FIRST_ROW), new String(rowName));
        System.out.println(rowsColumnRange.get(rowName));
    }

    @Test
    public void canGetAllTableNames(){
        Iterator<TableReference> it = db.getAllTableNames().iterator();
        assertEquals(TEST_TABLE, it.next());
        assertEquals(false, it.hasNext());
    }

    @Test
    public void canGetLatestTimestamps(){
        Map<Cell, Long> latest = db.getLatestTimestamps(TEST_TABLE, ImmutableMap.of(CELL_1_1, Long.MAX_VALUE));
        assertEquals(new Long(DEFAULT_TIMESTAMP), latest.get(CELL_1_1));
    }

    @Test
    public void getRangeOfTimestampsThrowsICE(){
        //Requires all Cassandra nodes to be available
        expect_exception.expect(InsufficientConsistencyException.class);
        final RangeRequest range = RangeRequest.builder().endRowExclusive(SECOND_ROW).build();
        ClosableIterator<RowResult<Set<Long>>> r = db.getRangeOfTimestamps(TEST_TABLE, range, Long.MAX_VALUE);
        while(r.hasNext()){
            System.out.println(r.next());
        }
    }


    @Test
    public void getAllTimestampsThrowsPRE(){
        expect_exception.expect(PalantirRuntimeException.class);
        Multimap<Cell, Long> result = db.getAllTimestamps(TEST_TABLE,
                ImmutableSet.of(CELL_1_1), Long.MAX_VALUE);
//        assertEquals(2, result.get(CELL_1_1).size());
    }

    protected static void verifyTimestampAndValue(Cell cell, long timestamp, byte[] value){
        Map<Cell, Value> result = db.get(TEST_TABLE, ImmutableMap.of(cell, Long.MAX_VALUE));
        assertEquals(timestamp, result.get(cell).getTimestamp());
        assertEquals(new String(value), new String(result.get(cell).getContents()));
    }
}
