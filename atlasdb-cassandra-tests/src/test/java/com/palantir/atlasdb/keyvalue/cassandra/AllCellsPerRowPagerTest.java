/*
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
 *
 */

package com.palantir.atlasdb.keyvalue.cassandra;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.StringEndsWith.endsWith;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Supplier;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;
import org.hamcrest.Matcher;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.util.Pair;

public class AllCellsPerRowPagerTest {

    private CqlExecutor executor = mock(CqlExecutor.class);
    private String rowName = "row";
    private ByteBuffer rowKey = toByteBuffer(rowName);
    private int pageSize = 20;

    private static final TableReference DEFAULT_TABLE = TableReference.fromString("tr");
    private static final String DEFAULT_COLUMN_NAME = "col1";
    private static final String PREVIOUS_COLUMN_NAME = "don't care";
    private static final long PREVIOUS_TIMESTAMP = 23;
    private static final List<ColumnOrSuperColumn> PREVIOUS_PAGE = ImmutableList.of(makeColumnOrSuperColumn(PREVIOUS_COLUMN_NAME, PREVIOUS_TIMESTAMP));

    private AllCellsPerRowPager pager = new AllCellsPerRowPager(
            executor,
            rowKey,
            DEFAULT_TABLE,
            pageSize
    );

    @Test
    public void getFirstPageShouldReturnSingleResult() {
        long timestamp = 1L;
        CqlRow row = makeCqlRow(DEFAULT_COLUMN_NAME, timestamp);
        allQueriesReturn(ImmutableList.of(row));

        verifySingletonListIsReturnedCorrectly(() -> pager.getFirstPage(), timestamp);
    }

    @Test
    public void getNextPageShouldReturnSingleResult() {
        long timestamp = 1L;
        CqlRow row = makeCqlRow(DEFAULT_COLUMN_NAME, timestamp);
        allQueriesReturn(ImmutableList.of(row));

        verifySingletonListIsReturnedCorrectly(() -> pager.getNextPage(PREVIOUS_PAGE), timestamp);
    }

    @Test
    public void getFirstPageShouldReturnMultipleResults() {
        CqlRow row1 = makeCqlRow(DEFAULT_COLUMN_NAME, 1L);
        CqlRow row2 = makeCqlRow(DEFAULT_COLUMN_NAME, 2L);
        allQueriesReturn(ImmutableList.of(row1, row2));

        verifyMultipleElementListIsReturnedCorrectly(() -> pager.getFirstPage());
    }

    @Test
    public void getNextPageShouldReturnMultipleResults() {
        CqlRow row1 = makeCqlRow(DEFAULT_COLUMN_NAME, 1L);
        CqlRow row2 = makeCqlRow(DEFAULT_COLUMN_NAME, 2L);
        allQueriesReturn(ImmutableList.of(row1, row2));

        verifyMultipleElementListIsReturnedCorrectly(() -> pager.getNextPage(PREVIOUS_PAGE));
    }

    @Test
    public void getFirstPageShouldExecuteQueryWithCorrectParameters() {
        allQueriesReturn(ImmutableList.of());

        pager.getFirstPage();

        verify(executor).getColAndTimestamp(
                DEFAULT_TABLE,
                encodeAsHex(rowKey.array()),
                pageSize);
    }

    @Test
    public void getNextPageShouldExecuteQueryWithCorrectParameters() {
        allQueriesReturn(ImmutableList.of());

        pager.getNextPage(PREVIOUS_PAGE);

        verify(executor).getColAndTimestampForColumnAndTimestamp(
                DEFAULT_TABLE,
                encodeAsHex(rowKey.array()),
                encodeAsHex(PREVIOUS_COLUMN_NAME.getBytes()),
                PREVIOUS_TIMESTAMP,
                pageSize);
    }

    private void verifySingletonListIsReturnedCorrectly(Supplier<List<ColumnOrSuperColumn>> method, long expectedTimestamp) {
        List<ColumnOrSuperColumn> page = method.get();

        assertThat(page, hasSize(1));
        assertColumnOrSuperColumnHasCorrectNameAndTimestamp(page.get(0), DEFAULT_COLUMN_NAME, expectedTimestamp);
    }

    private void verifyMultipleElementListIsReturnedCorrectly(Supplier<List<ColumnOrSuperColumn>> method) {
        List<ColumnOrSuperColumn> firstPage = method.get();

        assertThat(firstPage, hasSize(2));
    }

    private CqlRow makeCqlRow(String columnName, long timestamp) {
        List<Column> columns = ImmutableList.of(
                new Column().setValue(columnName.getBytes()),
                new Column().setValue(PtBytes.toBytes(~timestamp)));
        return new CqlRow(rowKey, columns);
    }

    private static ColumnOrSuperColumn makeColumnOrSuperColumn(String columnName, long timestamp) {
        return makeColumnOrSuperColumn(columnName.getBytes(), PtBytes.toBytes(timestamp));
    }

    private void allQueriesReturn(List<CqlRow> rows) {
        CqlResult cqlResult = mock(CqlResult.class);
        when(cqlResult.getRows()).thenReturn(rows);

        CqlResult noResults = mock(CqlResult.class);
        when(noResults.getRows()).thenReturn(ImmutableList.of());

        when(executor.getColAndTimestamp(any(TableReference.class), anyString(), anyInt())).thenReturn(cqlResult);
        when(executor.getColAndTimestampForColumnAndTimestamp(any(TableReference.class), anyString(), anyString(), anyLong(), anyInt())).thenReturn(cqlResult);
        when(executor.getColAndTimestampForNextColumn(any(TableReference.class), anyString(), anyString(), anyInt())).thenReturn(noResults);
    }

    private void assertColumnOrSuperColumnHasCorrectNameAndTimestamp(ColumnOrSuperColumn columnOrSuperColumn, String expectedName, long expectedTs) {
        Pair<byte[], Long> nameAndTimestamp = CassandraKeyValueServices.decomposeName(columnOrSuperColumn.getColumn());
        String colName = PtBytes.toString(nameAndTimestamp.getLhSide());
        assertThat(colName, equalTo(expectedName));

        long timestamp = nameAndTimestamp.getRhSide();
        assertThat(timestamp, equalTo(expectedTs));

    }

    private ByteBuffer toByteBuffer(String str) {
        return ByteBuffer.wrap(str.getBytes());
    }

    // TODO the below got copied from production code, which makes us sad
    private String encodeAsHex(byte[] array) {
        return "0x" + PtBytes.encodeHexString(array);
    }

    private static ColumnOrSuperColumn makeColumnOrSuperColumn(byte[] columnName, byte[] timestamp) {
        long timestampLong = ~PtBytes.toLong(timestamp);
        Column col = new Column()
                .setName(CassandraKeyValueServices.makeCompositeBuffer(columnName, timestampLong));
        return new ColumnOrSuperColumn().setColumn(col);
    }
}
