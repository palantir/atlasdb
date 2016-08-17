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
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
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

    @Test
    public void testGetFirstPage() {
        CqlExecutor executor = mock(CqlExecutor.class);
        ByteBuffer rowKey = toByteBuffer("row");
        AllCellsPerRowPager pager = new AllCellsPerRowPager(
                executor,
                rowKey,
                TableReference.fromString("tr"),
                10
        );

        List<Column> columns = ImmutableList.of(new Column().setValue("col1".getBytes()),
                new Column().setValue(PtBytes.toBytes(-2L)));
        CqlRow row = new CqlRow(rowKey, columns);

        CqlResult cqlResult = mock(CqlResult.class);
        when(cqlResult.getRows()).thenReturn(ImmutableList.of(row));
        when(executor.execute(anyString())).thenReturn(cqlResult);

        List<ColumnOrSuperColumn> firstPage = pager.getFirstPage();

        assertThat(firstPage, hasSize(1));
        ColumnOrSuperColumn columnOrSuperColumn = firstPage.get(0);
        Pair<byte[], Long> nameAndTimestamp = CassandraKeyValueServices.decomposeName(columnOrSuperColumn.getColumn());
        String colName = PtBytes.toString(nameAndTimestamp.getLhSide());
        assertThat(colName, equalTo("col1"));

        long timestamp = nameAndTimestamp.getRhSide();
        assertThat(timestamp, equalTo(1L));
   }

    private ByteBuffer toByteBuffer(String str) {
        return ByteBuffer.wrap(str.getBytes());
    }

}