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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Multimap;
import com.google.common.primitives.UnsignedBytes;
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

    ImmutableMap<Cell, Value> expectedRow = ImmutableMap.of(
            OneNodeDownTestSuite.CELL_1_1, OneNodeDownTestSuite.DEFAULT_VALUE,
            OneNodeDownTestSuite.CELL_1_2, OneNodeDownTestSuite.DEFAULT_VALUE);

    @Test
    public void canGet() {
        OneNodeDownTestSuite.verifyValue(OneNodeDownTestSuite.CELL_1_1,
                OneNodeDownTestSuite.DEFAULT_VALUE);
    }

    @Test
    public void canGetRows() {
        Map<Cell, Value> row = OneNodeDownTestSuite.db.getRows(OneNodeDownTestSuite.TEST_TABLE,
                ImmutableList.of(OneNodeDownTestSuite.FIRST_ROW), ColumnSelection.all(), Long.MAX_VALUE);

        assertThat(row).containsAllEntriesOf(expectedRow);
    }

    @Test
    public void canGetRange() {
        final RangeRequest range = RangeRequest.builder().endRowExclusive(OneNodeDownTestSuite.SECOND_ROW).build();
        ClosableIterator<RowResult<Value>> it = OneNodeDownTestSuite.db.getRange(OneNodeDownTestSuite.TEST_TABLE,
                range, Long.MAX_VALUE);

        ImmutableMap<byte[], Value> expectedColumns = ImmutableMap.of(
                OneNodeDownTestSuite.FIRST_COLUMN, OneNodeDownTestSuite.DEFAULT_VALUE,
                OneNodeDownTestSuite.SECOND_COLUMN, OneNodeDownTestSuite.DEFAULT_VALUE);
        RowResult<Value> expectedRowResult = RowResult.create(OneNodeDownTestSuite.FIRST_ROW,
                ImmutableSortedMap.copyOf(expectedColumns, UnsignedBytes.lexicographicalComparator()));

        assertThat(it).containsExactly(expectedRowResult);
    }

    @Test
    public void canGetRowsColumnRange() {
        BatchColumnRangeSelection rangeSelection = BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY,
                PtBytes.EMPTY_BYTE_ARRAY, 1);
        Map<byte[], RowColumnRangeIterator> rowsColumnRange = OneNodeDownTestSuite.db.getRowsColumnRange(
                OneNodeDownTestSuite.TEST_TABLE, ImmutableList.of(OneNodeDownTestSuite.FIRST_ROW),
                rangeSelection, Long.MAX_VALUE);

        assertEquals(1, rowsColumnRange.size());
        byte[] rowName = rowsColumnRange.entrySet().iterator().next().getKey();
        assertTrue(Arrays.equals(OneNodeDownTestSuite.FIRST_ROW, rowName));

        RowColumnRangeIterator it = rowsColumnRange.get(rowName);
        assertThat(it).containsExactlyElementsOf(expectedRow.entrySet());
    }

    @Test
    public void canGetAllTableNames() {
        Iterator<TableReference> it = OneNodeDownTestSuite.db.getAllTableNames().iterator();
        assertThat(it).contains(OneNodeDownTestSuite.TEST_TABLE);
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

        ImmutableMap<byte[], ImmutableSet<Long>> expectedTimestamps = ImmutableMap.of(
                OneNodeDownTestSuite.FIRST_COLUMN,
                ImmutableSet.of(OneNodeDownTestSuite.DEFAULT_TIMESTAMP, OneNodeDownTestSuite.OLD_TIMESTAMP),
                OneNodeDownTestSuite.SECOND_COLUMN,
                ImmutableSet.of(OneNodeDownTestSuite.DEFAULT_TIMESTAMP));
        RowResult<Set<Long>> expectedRowResult = RowResult.create(OneNodeDownTestSuite.FIRST_ROW,
                ImmutableSortedMap.copyOf(expectedTimestamps, UnsignedBytes.lexicographicalComparator()));

        assertThat(it).containsExactly(expectedRowResult);
    }


    @Test
    public void canGetAllTimestamps() {
        Multimap<Cell, Long> result = OneNodeDownTestSuite.db.getAllTimestamps(OneNodeDownTestSuite.TEST_TABLE,
                ImmutableSet.of(OneNodeDownTestSuite.CELL_1_1), Long.MAX_VALUE);

        ImmutableMultimap<Cell, Long> expectedResult = ImmutableMultimap.<Cell, Long>builder()
                .put(OneNodeDownTestSuite.CELL_1_1, OneNodeDownTestSuite.OLD_TIMESTAMP)
                .put(OneNodeDownTestSuite.CELL_1_1, OneNodeDownTestSuite.DEFAULT_TIMESTAMP)
                .build();

        assertThat(result.entries()).containsExactlyElementsOf(expectedResult.entries());
    }
}
