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

package com.palantir.atlasdb.keyvalue.cassandra;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Supplier;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.util.Pair;

public class AllCellsPerRowPagerTest {
    private CqlExecutor executor = mock(CqlExecutor.class);
    private ByteBuffer rowKey = toByteBuffer("row");
    private int pageSize = 20;

    private static final TableReference DEFAULT_TABLE = TableReference.fromString("tr");
    private static final String DEFAULT_COLUMN_NAME = "col1";
    private static final String OTHER_COLUMN_NAME = "otherColumn";
    private static final String PREVIOUS_COLUMN_NAME = "don't care";
    private static final long PREVIOUS_TIMESTAMP = 23;

    // ColumnOrSuperColumn has inverted timestamps
    private static final List<ColumnOrSuperColumn> PREVIOUS_PAGE = ImmutableList.of(
            makeColumnOrSuperColumn(PREVIOUS_COLUMN_NAME, ~PREVIOUS_TIMESTAMP));

    private AllCellsPerRowPager pager = new AllCellsPerRowPager(
            executor,
            rowKey,
            DEFAULT_TABLE,
            pageSize
    );

    @Test
    public void getFirstPageShouldReturnSingleResult() {
        long timestamp = 1L;
        CellWithTimestamp cell = makeCell(DEFAULT_COLUMN_NAME, timestamp);
        allQueriesReturn(ImmutableList.of(cell));

        verifySingletonListIsReturnedCorrectly(() -> pager.getFirstPage(), timestamp);
    }

    @Test
    public void getNextPageShouldReturnSingleResult() {
        long timestamp = 1L;
        CellWithTimestamp cell = makeCell(DEFAULT_COLUMN_NAME, timestamp);
        allQueriesReturn(ImmutableList.of(cell));

        verifySingletonListIsReturnedCorrectly(() -> pager.getNextPage(PREVIOUS_PAGE), timestamp);
    }

    @Test
    public void getFirstPageShouldReturnMultipleResults() {
        CellWithTimestamp cell1 = makeCell(DEFAULT_COLUMN_NAME, 1L);
        CellWithTimestamp cell2 = makeCell(DEFAULT_COLUMN_NAME, 2L);
        allQueriesReturn(ImmutableList.of(cell1, cell2));

        verifyMultipleElementListIsReturnedCorrectly(() -> pager.getFirstPage());
    }

    @Test
    public void getNextPageShouldReturnMultipleResults() {
        CellWithTimestamp cell1 = makeCell(DEFAULT_COLUMN_NAME, 1L);
        CellWithTimestamp cell2 = makeCell(DEFAULT_COLUMN_NAME, 2L);
        allQueriesReturn(ImmutableList.of(cell1, cell2));

        verifyMultipleElementListIsReturnedCorrectly(() -> pager.getNextPage(PREVIOUS_PAGE));
    }

    @Test
    public void getFirstPageShouldExecuteQueryWithCorrectParameters() {
        allQueriesReturn(ImmutableList.of());

        pager.getFirstPage();

        verify(executor).getColumnsForRow(
                DEFAULT_TABLE,
                rowKey.array(),
                pageSize);
    }

    @Test
    public void getNextPageShouldExecuteQueryWithCorrectParameters() {
        allQueriesReturn(ImmutableList.of());

        pager.getNextPage(PREVIOUS_PAGE);

        verify(executor).getTimestampsForRowAndColumn(
                DEFAULT_TABLE,
                rowKey.array(),
                PREVIOUS_COLUMN_NAME.getBytes(StandardCharsets.UTF_8),
                PREVIOUS_TIMESTAMP,
                pageSize);
    }

    @Test
    public void getNextPageShouldSpanMultipleColumns() {
        CellWithTimestamp cell1 = makeCell(DEFAULT_COLUMN_NAME, 10L);
        CellWithTimestamp cell2 = makeCell(OTHER_COLUMN_NAME, 20L);

        allQueriesWithColumnAndTimestampReturn(ImmutableList.of(cell1));
        allQueriesWithColumnReturn(ImmutableList.of(cell2));

        List<ColumnOrSuperColumn> nextPage = pager.getNextPage(PREVIOUS_PAGE);

        assertColumnOrSuperColumnHasCorrectNameAndTimestamp(nextPage.get(0), DEFAULT_COLUMN_NAME, 10L);
        assertColumnOrSuperColumnHasCorrectNameAndTimestamp(nextPage.get(1), OTHER_COLUMN_NAME, 20L);
    }

    private void verifySingletonListIsReturnedCorrectly(
            Supplier<List<ColumnOrSuperColumn>> method,
            long expectedTimestamp) {
        List<ColumnOrSuperColumn> page = method.get();

        assertThat(page, hasSize(1));
        assertColumnOrSuperColumnHasCorrectNameAndTimestamp(page.get(0), DEFAULT_COLUMN_NAME, expectedTimestamp);
    }

    private void verifyMultipleElementListIsReturnedCorrectly(Supplier<List<ColumnOrSuperColumn>> method) {
        List<ColumnOrSuperColumn> firstPage = method.get();

        assertThat(firstPage, hasSize(2));
    }

    private CellWithTimestamp makeCell(String columnName, long timestamp) {
        return new CellWithTimestamp.Builder()
                .cell(Cell.create(rowKey.array(), columnName.getBytes(StandardCharsets.UTF_8)))
                .timestamp(timestamp)
                .build();
    }

    private static ColumnOrSuperColumn makeColumnOrSuperColumn(String columnName, long timestamp) {
        long timestampLong = ~PtBytes.toLong(PtBytes.toBytes(timestamp));
        ByteBuffer name = CassandraKeyValueServices.makeCompositeBuffer(
                columnName.getBytes(StandardCharsets.UTF_8), timestampLong);
        Column col = new Column().setName(name);
        return new ColumnOrSuperColumn().setColumn(col);
    }

    private void allQueriesReturn(List<CellWithTimestamp> cells) {
        allQueriesSimpleReturn(cells);
        allQueriesWithColumnAndTimestampReturn(cells);
        allQueriesWithColumnReturn(ImmutableList.of());
    }

    private void allQueriesSimpleReturn(List<CellWithTimestamp> cells) {
        when(executor.getColumnsForRow(any(TableReference.class), any(byte[].class), anyInt())).thenReturn(cells);
    }

    private void allQueriesWithColumnAndTimestampReturn(List<CellWithTimestamp> cells) {
        when(executor.getTimestampsForRowAndColumn(
                any(TableReference.class), any(byte[].class), any(byte[].class), anyLong(), anyInt()))
                .thenReturn(cells);
    }

    private void allQueriesWithColumnReturn(List<CellWithTimestamp> cells) {
        when(executor.getNextColumnsForRow(
                any(TableReference.class), any(byte[].class), any(byte[].class), anyInt())).thenReturn(cells);
    }

    private void assertColumnOrSuperColumnHasCorrectNameAndTimestamp(
            ColumnOrSuperColumn columnOrSuperColumn,
            String expectedName,
            long expectedTs) {
        Pair<byte[], Long> nameAndTimestamp = CassandraKeyValueServices.decomposeName(columnOrSuperColumn.getColumn());
        String colName = PtBytes.toString(nameAndTimestamp.getLhSide());
        assertThat(colName, equalTo(expectedName));

        long timestamp = nameAndTimestamp.getRhSide();
        assertThat(timestamp, equalTo(expectedTs));
    }

    private ByteBuffer toByteBuffer(String str) {
        return ByteBuffer.wrap(str.getBytes(StandardCharsets.UTF_8));
    }
}
