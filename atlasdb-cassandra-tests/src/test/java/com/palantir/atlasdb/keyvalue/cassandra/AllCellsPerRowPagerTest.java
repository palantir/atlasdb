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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.StringEndsWith.endsWith;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.util.Pair;

public class AllCellsPerRowPagerTest {

    CqlExecutor executor = mock(CqlExecutor.class);
    ByteBuffer rowKey = toByteBuffer("row");
    int pageSize = 20;

    AllCellsPerRowPager pager = new AllCellsPerRowPager(
            executor,
            rowKey,
            TableReference.fromString("tr"),
            pageSize
    );

    @Test
    public void testGetFirstPage() {
        String columnName = "col1";
        List<Column> columns = ImmutableList.of(
                new Column().setValue(columnName.getBytes()),
                new Column().setValue(PtBytes.toBytes(-2L)));
        CqlRow row = new CqlRow(rowKey, columns);

        allQueriesReturn(ImmutableList.of(row));

        List<ColumnOrSuperColumn> firstPage = pager.getFirstPage();

        assertThat(firstPage, hasSize(1));
        assertColumnOrSuperColumnHasCorrectNameAndTimestamp(firstPage.get(0), columnName, 1L);
    }

    @Test
    public void getFirstPageShouldExecuteQueryLimitedToPageSize() {
        allQueriesReturn(ImmutableList.of());

        pager.getFirstPage();

        verify(executor).execute(argThat(endsWith(String.format("LIMIT %s;", pageSize))));
    }

    private void allQueriesReturn(List<CqlRow> rows) {
        CqlResult cqlResult = mock(CqlResult.class);
        when(cqlResult.getRows()).thenReturn(rows);
        when(executor.execute(anyString())).thenReturn(cqlResult);
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

}