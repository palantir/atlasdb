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
 *
 */

package com.palantir.atlasdb.keyvalue.cassandra;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.thrift.TException;

import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.common.base.Throwables;

public class CqlExecutor {
    private CassandraClientPool clientPool;
    private ConsistencyLevel consistency;

    CqlExecutor(CassandraClientPool clientPool, ConsistencyLevel consistency) {
        this.clientPool = clientPool;
        this.consistency = consistency;
    }

    /**
     * @param tableRef the table from which to select
     * @param row the row key
     * @param limit the maximum number of results to return.
     * @return up to <code>limit</code> cells that match the row name
     */
    List<CellWithTimestamp> getColumnsForRow(TableReference tableRef, byte[] row, int limit) {
        String query = String.format(
                "SELECT column1, column2 FROM %s WHERE key = %s LIMIT %s;",
                getTableName(tableRef),
                CassandraKeyValueServices.encodeAsHex(row),
                limit);
        CqlResult cqlResult = executeQueryOnHost(query, getHostForRow(row));
        return getCells(row, cqlResult);
    }

    /**
     * @param tableRef the table from which to select
     * @param row the row key
     * @param column the column name
     * @param maxTimestampExclusive the maximum timestamp, exclusive
     * @param limit the maximum number of results to return.
     * @return up to <code>limit</code> cells that exactly match the row and column name,
     * and have a timestamp less than <code>maxTimestampExclusive</code>
     */
    List<CellWithTimestamp> getTimestampsForRowAndColumn(
            TableReference tableRef,
            byte[] row,
            String column,
            long maxTimestampExclusive,
            int limit) {
        long invertedTimestamp = ~maxTimestampExclusive;
        String query = String.format(
                "SELECT column1, column2 FROM %s WHERE key = %s AND column1 = %s AND column2 > %s LIMIT %s;",
                getTableName(tableRef),
                CassandraKeyValueServices.encodeAsHex(row),
                column,
                invertedTimestamp,
                limit);
        CqlResult cqlResult = executeQueryOnHost(query, getHostForRow(row));
        return getCells(row, cqlResult);
    }

    /**
     * @param tableRef the table from which to select
     * @param row the row key
     * @param previousColumn the lexicographic lower bound (exclusive) for the column name
     * @param limit the maximum number of results to return.
     * @return up to <code>limit</code> results where the column name is lexicographically later than the supplied
     * <code>previousColumn</code>. Note that this can return results from multiple columns
     */
    List<CellWithTimestamp> getNextColumnsForRow(
            TableReference tableRef,
            byte[] row,
            String previousColumn,
            int limit) {
        String query = String.format(
                "SELECT column1, column2 FROM %s WHERE key = %s AND column1 > %s LIMIT %s;",
                getTableName(tableRef),
                CassandraKeyValueServices.encodeAsHex(row),
                previousColumn,
                limit);
        CqlResult cqlResult = executeQueryOnHost(query, getHostForRow(row));
        return getCells(row, cqlResult);
    }

    private InetSocketAddress getHostForRow(byte[] row) {
        return clientPool.getRandomHostForKey(row);
    }

    private CqlResult executeQueryOnHost(String query, InetSocketAddress host) {
        ByteBuffer queryBytes = ByteBuffer.wrap(query.getBytes(StandardCharsets.UTF_8));
        return executeQueryOnHost(queryBytes, host);
    }

    private CqlResult executeQueryOnHost(ByteBuffer queryBytes, InetSocketAddress host) {
        try {
            return clientPool.runWithRetryOnHost(host, client ->
                    client.execute_cql3_query(queryBytes, Compression.NONE, consistency));
        } catch (TException e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    private String getTableName(TableReference tableRef) {
        return CassandraKeyValueService.internalTableName(tableRef);
    }

    private List<CellWithTimestamp> getCells(byte[] key, CqlResult cqlResult) {
        return cqlResult.getRows().stream().map(cqlRow -> getCell(key, cqlRow)).collect(Collectors.toList());
    }

    private CellWithTimestamp getCell(byte[] key, CqlRow cqlRow) {
        byte[] columnName = cqlRow.getColumns().get(0).getValue();
        byte[] flippedTimestampAsBytes = cqlRow.getColumns().get(1).getValue();
        long timestampLong = ~PtBytes.toLong(flippedTimestampAsBytes);

        return new CellWithTimestamp.Builder().cell(Cell.create(key, columnName)).timestamp(timestampLong).build();
    }
}
