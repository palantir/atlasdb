/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.containers.CassandraContainer;
import com.palantir.atlasdb.containers.Containers;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.paging.CassandraRawCellValue;
import com.palantir.atlasdb.keyvalue.cassandra.paging.CellPager;
import com.palantir.atlasdb.keyvalue.cassandra.paging.CellPagerBatchSizingStrategy;
import com.palantir.atlasdb.keyvalue.cassandra.paging.ImmutableCassandraRawCellValue;
import com.palantir.atlasdb.keyvalue.cassandra.paging.SingleRowColumnPager;
import com.palantir.atlasdb.keyvalue.impl.TestDataBuilder;
import com.palantir.atlasdb.keyvalue.impl.TracingPrefsConfig;

public class CellPagerIntegrationTest {
    protected static final TableReference TEST_TABLE = TableReference.createFromFullyQualifiedName(
            "cell_pager.test_table");

    @ClassRule
    public static final Containers CONTAINERS = new Containers(CellPagerIntegrationTest.class)
            .with(new CassandraContainer());

    private static CassandraKeyValueService kvs = null;
    private CellPagerBatchSizingStrategy pageSizingStrategy = Mockito.mock(CellPagerBatchSizingStrategy.class);
    private CellPager cellPager = null;

    @BeforeClass
    public static void setUpKvs() {
        kvs = CassandraKeyValueService.create(
                CassandraKeyValueServiceConfigManager.createSimpleManager(CassandraContainer.KVS_CONFIG),
                CassandraContainer.LEADER_CONFIG,
                Mockito.mock(Logger.class));
    }

    @Before
    public void setUp() {
        kvs.createTable(TEST_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);
        kvs.truncateTable(TEST_TABLE);
        TracingQueryRunner queryRunner = new TracingQueryRunner(
                Mockito.mock(Logger.class),
                new TracingPrefsConfig());
        SingleRowColumnPager singleRowPager = new SingleRowColumnPager(kvs.clientPool, queryRunner);
        cellPager = new CellPager(singleRowPager, kvs.clientPool, queryRunner, pageSizingStrategy);
    }

    @Test
    public void testPaging() {
        // Row 1:  5 cells
        // Row 2:  1 cell
        // Row 3:  2 cells
        // Row 4:  1 cell
        // Row 5:  1 cell
        testDataBuilder()
                .put(1, 1, 30L, "a")
                .put(1, 1, 20L, "b")
                .put(1, 1, 10L, "c")
                .put(1, 2, 20L, "d")
                .put(1, 2, 10L, "e")
                .put(2, 3, 10L, "f")
                .put(3, 1, 20L, "g")
                .put(3, 1, 10L, "h")
                .put(4, 2, 10L, "i")
                .put(5, 2, 10L, "j")
                .store();
        Mockito.doReturn(new CellPagerBatchSizingStrategy.PageSizes(3, 2))
                .when(pageSizingStrategy).computePageSizes(Mockito.anyInt(), Mockito.any());
        List<List<CassandraRawCellValue>> pages = ImmutableList.copyOf(cellPager.createCellIterator(
                TEST_TABLE, PtBytes.EMPTY_BYTE_ARRAY, 2, ConsistencyLevel.ALL));
        assertEquals(
                ImmutableList.of(
                    // incomplete part of row 1 fetched with get_range_slice
                    ImmutableList.of(val(1, 1, 30L, "a"), val(1, 1, 20L, "b")),
                    // first page of the remainder of row 1 fetched with get_slice
                    ImmutableList.of(val(1, 1, 10L, "c"), val(1, 2, 20L, "d")),
                    // second page of the remainder of row 1 fetched with get_slice
                    ImmutableList.of(val(1, 2, 10L, "e")),
                    // remaining rows already fetched with get_range_slice
                    ImmutableList.of(val(2, 3, 10L, "f"), val(3, 1, 20L, "g"), val(3, 1, 10L, "h")),
                    // the second get_range_slice
                    ImmutableList.of(val(4, 2, 10L, "i"), val(5, 2, 10L, "j"))),
                pages);
    }

    @Test
    public void testStartRow() {
        testDataBuilder()
                .put(1, 1, 10L, "foo")
                .put(2, 1, 10L, "bar")
                .store();
        Mockito.doReturn(new CellPagerBatchSizingStrategy.PageSizes(3, 2))
                .when(pageSizingStrategy).computePageSizes(Mockito.anyInt(), Mockito.any());
        List<List<CassandraRawCellValue>> pages = ImmutableList.copyOf(cellPager.createCellIterator(
                TEST_TABLE, TestDataBuilder.row(2), 2, ConsistencyLevel.ALL));
        assertEquals(ImmutableList.of(ImmutableList.of(val(2, 1, 10L, "bar"))), pages);
    }

    private static CassandraRawCellValue val(int row, int col, long ts, String value) {
        Column column = new Column();
        column.setName(CassandraKeyValueServices.makeCompositeBuffer(TestDataBuilder.row(col), ts));
        column.setValue(TestDataBuilder.value(value));
        column.setTimestamp(ts);
        return ImmutableCassandraRawCellValue.builder()
                .rowKey(TestDataBuilder.row(row))
                .column(column)
                .build();
    }

    private TestDataBuilder testDataBuilder() {
        return new TestDataBuilder(kvs, TEST_TABLE);
    }

}
