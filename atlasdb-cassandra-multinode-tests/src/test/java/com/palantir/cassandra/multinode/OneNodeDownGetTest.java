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
package com.palantir.cassandra.multinode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueService;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.exception.AtlasDbDependencyException;
import java.util.Map;
import java.util.Set;
import org.junit.Test;

public class OneNodeDownGetTest extends AbstractDegradedClusterTest {
    private static final Set<Map.Entry<Cell, Value>> expectedRowEntries =
            ImmutableMap.of(CELL_1_1, VALUE, CELL_1_2, VALUE).entrySet();

    @Override
    void testSetup(CassandraKeyValueService kvs) {
        kvs.createTable(TEST_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);
        kvs.put(TEST_TABLE, ImmutableMap.of(CELL_1_1, PtBytes.toBytes("old_value")), TIMESTAMP - 1);
        kvs.put(TEST_TABLE, ImmutableMap.of(CELL_1_1, CONTENTS), TIMESTAMP);
        kvs.put(TEST_TABLE, ImmutableMap.of(CELL_1_2, CONTENTS), TIMESTAMP);
        kvs.put(TEST_TABLE, ImmutableMap.of(CELL_2_1, CONTENTS), TIMESTAMP);
    }

    @Test
    public void canGet() {
        assertLatestValueInCellEquals(CELL_1_1, VALUE);
    }

    @Test
    public void canGetRows() {
        Map<Cell, Value> row =
                getTestKvs().getRows(TEST_TABLE, ImmutableList.of(FIRST_ROW), ColumnSelection.all(), Long.MAX_VALUE);

        assertThat(row.entrySet()).hasSameElementsAs(expectedRowEntries);
    }

    @Test
    public void canGetRange() {
        RangeRequest range = RangeRequest.builder().endRowExclusive(SECOND_ROW).build();
        ClosableIterator<RowResult<Value>> resultIterator = getTestKvs().getRange(TEST_TABLE, range, Long.MAX_VALUE);

        Map<byte[], Value> expectedColumns = ImmutableMap.of(FIRST_COLUMN, VALUE, SECOND_COLUMN, VALUE);
        RowResult<Value> expectedRowResult = RowResult.create(
                FIRST_ROW, ImmutableSortedMap.copyOf(expectedColumns, UnsignedBytes.lexicographicalComparator()));

        assertThat(resultIterator).toIterable().containsExactlyElementsOf(ImmutableList.of(expectedRowResult));
    }

    @Test
    public void canGetRowsColumnRange() {
        BatchColumnRangeSelection rangeSelection = BatchColumnRangeSelection.create(null, null, 1);
        Map<byte[], RowColumnRangeIterator> rowsColumnRange = getTestKvs()
                .getRowsColumnRange(TEST_TABLE, ImmutableList.of(FIRST_ROW), rangeSelection, Long.MAX_VALUE);

        assertThat(Iterables.getOnlyElement(rowsColumnRange.keySet())).isEqualTo(FIRST_ROW);
        assertThat(rowsColumnRange.get(FIRST_ROW)).toIterable().containsExactlyElementsOf(expectedRowEntries);
    }

    @Test
    public void canGetAllTableNames() {
        assertThat(getTestKvs().getAllTableNames()).contains(TEST_TABLE);
    }

    @Test
    public void canGetLatestTimestamps() {
        Map<Cell, Long> latestTs =
                getTestKvs().getLatestTimestamps(TEST_TABLE, ImmutableMap.of(CELL_1_1, Long.MAX_VALUE));
        assertThat(latestTs.get(CELL_1_1).longValue()).isEqualTo(TIMESTAMP);
    }

    @Test
    public void getRangeOfTimestampsThrows() {
        RangeRequest range = RangeRequest.builder().endRowExclusive(SECOND_ROW).build();
        try (ClosableIterator<RowResult<Set<Long>>> resultIterator =
                getTestKvs().getRangeOfTimestamps(TEST_TABLE, range, Long.MAX_VALUE)) {
            assertThatThrownBy(resultIterator::next).isInstanceOf(AtlasDbDependencyException.class);
        }
    }

    @Test
    public void getAllTimestampsThrows() {
        assertThatThrownBy(() -> getTestKvs().getAllTimestamps(TEST_TABLE, ImmutableSet.of(CELL_1_1), Long.MAX_VALUE))
                .isInstanceOf(AtlasDbDependencyException.class);
    }

    private void assertLatestValueInCellEquals(Cell cell, Value value) {
        Map<Cell, Value> result = getTestKvs().get(TEST_TABLE, ImmutableMap.of(cell, Long.MAX_VALUE));
        assertThat(result).containsEntry(cell, value);
    }
}
