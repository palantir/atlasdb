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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueService;
import com.palantir.common.base.ClosableIterator;
import org.junit.Test;

public class TwoNodesDownGetTest extends AbstractDegradedClusterTest {

    @Override
    void testSetup(CassandraKeyValueService kvs) {
        kvs.createTable(TEST_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);
        kvs.put(TEST_TABLE, ImmutableMap.of(CELL_1_1, PtBytes.toBytes("old_value")), TIMESTAMP - 1);
        kvs.put(TEST_TABLE, ImmutableMap.of(CELL_1_1, CONTENTS), TIMESTAMP);
        kvs.put(TEST_TABLE, ImmutableMap.of(CELL_1_2, CONTENTS), TIMESTAMP);
        kvs.put(TEST_TABLE, ImmutableMap.of(CELL_2_1, CONTENTS), TIMESTAMP);
    }

    @Test
    public void getThrows() {
        assertThrowsAtlasDbDependencyExceptionAndDoesNotChangeCassandraSchema(
                () -> getTestKvs().get(TEST_TABLE, ImmutableMap.of(CELL_1_1, Long.MAX_VALUE)));
    }

    @Test
    public void getRowsThrows() {
        assertThrowsAtlasDbDependencyExceptionAndDoesNotChangeCassandraSchema(() ->
                getTestKvs().getRows(TEST_TABLE, ImmutableList.of(FIRST_ROW), ColumnSelection.all(), Long.MAX_VALUE));
    }

    @Test
    public void getRangeThrows() {
        RangeRequest range = RangeRequest.builder().endRowExclusive(SECOND_ROW).build();
        ClosableIterator<RowResult<Value>> resultIterator = getTestKvs().getRange(TEST_TABLE, range, Long.MAX_VALUE);

        assertThrowsAtlasDbDependencyExceptionAndDoesNotChangeCassandraSchema(resultIterator::hasNext);
    }

    @Test
    public void getRowsColumnRangeThrows() {
        BatchColumnRangeSelection rangeSelection = BatchColumnRangeSelection.create(null, null, 1);
        assertThrowsAtlasDbDependencyExceptionAndDoesNotChangeCassandraSchema(() -> getTestKvs()
                .getRowsColumnRange(TEST_TABLE, ImmutableList.of(FIRST_ROW), rangeSelection, Long.MAX_VALUE));
    }

    @Test
    public void getAllTableNamesThrows() {
        assertThrowsAtlasDbDependencyExceptionAndDoesNotChangeCassandraSchema(getTestKvs()::getAllTableNames);
    }

    @Test
    public void getLatestTimestampsThrows() {
        assertThrowsAtlasDbDependencyExceptionAndDoesNotChangeCassandraSchema(
                () -> getTestKvs().getLatestTimestamps(TEST_TABLE, ImmutableMap.of(CELL_1_1, Long.MAX_VALUE)));
    }
}
