/**
 * Copyright 2016 Palantir Technologies
 * <p>
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://opensource.org/licenses/BSD-3-Clause
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.cassandra;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.thrift.TException;

import com.google.common.base.Charsets;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.common.base.Throwables;
import com.palantir.util.Pair;
import com.palantir.util.paging.PageGetter;

public class AllCellsPerRowPager implements PageGetter<ColumnOrSuperColumn> {

    private CassandraClientPool clientPool;
    private InetSocketAddress host;
    private String row;
    private TableReference tableRef;
    private ConsistencyLevel consistency;
    private int pageSize;

    public AllCellsPerRowPager(CassandraClientPool clientPool, InetSocketAddress host, ByteBuffer row, TableReference tableRef, ConsistencyLevel consistency, int pageSize) {
        this.clientPool = clientPool;
        this.host = host;
        this.row = encodeAsHex(row.array());
        this.tableRef = tableRef;
        this.consistency = consistency;
        this.pageSize = pageSize;
    }

    @Override
    public List<ColumnOrSuperColumn> getFirstPage() {
        String query = String.format(
                "select column1, column2 from %s where key = %s LIMIT 10;",
                getTableName(),
                row);

        return getColumns(query);
    }

    @Override
    public List<ColumnOrSuperColumn> getNextPage(List<ColumnOrSuperColumn> currentPage) {
        ColumnOrSuperColumn previousResult = Iterables.getLast(currentPage);

        Pair<byte[], Long> nameAndTimestamp = CassandraKeyValueServices.decomposeName(previousResult.getColumn());
        String columnNameStr = encodeAsHex(nameAndTimestamp.getLhSide());
        long timestamp = ~nameAndTimestamp.getRhSide();

        String query = String.format(
                "select column1, column2 from %s where key = %s AND column1 = %s AND column2 > %s LIMIT 10;",
                getTableName(),
                row,
                columnNameStr,
                timestamp);

        return getColumns(query);
    }

    @Override
    public int getPageSize() {
        return pageSize;
    }

    private static String encodeAsHex(byte[] array) {
        return "0x" + PtBytes.encodeHexString(array);
    }

    private String getTableName() {
        return CassandraKeyValueService.internalTableName(tableRef);
    }

    private List<ColumnOrSuperColumn> getColumns(String query) {
        ByteBuffer queryBytes = ByteBuffer.wrap(query.getBytes(Charsets.UTF_8));
        CqlResult cqlResult = executeQuery(queryBytes);

        return getColumns(cqlResult);
    }

    private CqlResult executeQuery(ByteBuffer queryBytes) {
        try {
            return clientPool.runWithRetryOnHost(host, client -> client.execute_cql3_query(queryBytes, Compression.NONE, consistency));
        } catch (TException e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    private List<ColumnOrSuperColumn> getColumns(CqlResult cqlResult) {
        return cqlResult.getRows().stream()
                .map(this::getColumn)
                .collect(Collectors.toList());
    }

    private ColumnOrSuperColumn getColumn(CqlRow cqlRow) {
        byte[] columnName = cqlRow.getColumns().get(0).getValue();
        byte[] timestampAsBytes = cqlRow.getColumns().get(1).getValue();

        return makeColumnOrSuperColumn(columnName, timestampAsBytes);
    }

    private ColumnOrSuperColumn makeColumnOrSuperColumn(byte[] columnName, byte[] timestamp) {
        long timestampLong = ~PtBytes.toLong(timestamp);
        Column col = new Column()
                .setName(CassandraKeyValueServices.makeCompositeBuffer(columnName, timestampLong));
        return new ColumnOrSuperColumn().setColumn(col);
    }
}
