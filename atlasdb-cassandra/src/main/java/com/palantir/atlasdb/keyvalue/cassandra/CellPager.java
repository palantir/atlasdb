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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.thrift.TException;

import com.google.common.base.Charsets;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.TableReference;

public class CellPager {

    private final CassandraClientPool clientPool;
    private final InetSocketAddress host;

    public CellPager(CassandraClientPool clientPool, InetSocketAddress host) {
        this.clientPool = clientPool;
        this.host = host;
    }

    public Map<ByteBuffer, List<ColumnOrSuperColumn>> getColsByKeyWithPaging(Set<ByteBuffer> rows, TableReference tableRef, ConsistencyLevel consistency) throws TException {
        Map<ByteBuffer, List<ColumnOrSuperColumn>> colsByKey = new HashMap<>();

        for (ByteBuffer row : rows) {
            List<ColumnOrSuperColumn> columns = new ArrayList<>();

            String rowAsByteString = "0x" + PtBytes.encodeHexString(row.array());
            String tableName = CassandraKeyValueService.internalTableName(tableRef);
            String columnNameStr = "0x00";
            String query = "select column1, column2 from " + tableName + " where key = " + rowAsByteString + " AND column1 > " + columnNameStr + " LIMIT 10;";
            System.out.println("query (first): " + query);

            while (true) {
                ByteBuffer queryBytes = ByteBuffer.wrap(query.getBytes(Charsets.UTF_8));

                CqlResult cqlResult = clientPool.runWithRetryOnHost(host, client -> client.execute_cql3_query(queryBytes, Compression.NONE, consistency));

                long timestamp = 0;
                for (CqlRow cqlRow : cqlResult.getRows()) {
                    byte[] columnName = cqlRow.getColumns().get(0).getValue();
                    byte[] timestampAsBytes = cqlRow.getColumns().get(1).getValue();
                    timestamp = PtBytes.toLong(timestampAsBytes);
                    columnNameStr = "0x" + PtBytes.encodeHexString(columnName);

                    ColumnOrSuperColumn columnOrSuperColumn = makeColumnOrSuperColumn(columnName, timestampAsBytes);
                    columns.add(columnOrSuperColumn);
                }

                if (cqlResult.getRows().size() < 10) {
                    break;
                }
                query = "select column1, column2 from " + tableName + " where key = " + rowAsByteString + " AND column1 = " + columnNameStr + " AND column2 > " + timestamp + " LIMIT 10;";
                System.out.println("query: " + query);
            }

            colsByKey.put(row, columns);
        }
        return colsByKey;
    }

    private ColumnOrSuperColumn makeColumnOrSuperColumn(byte[] columnName, byte[] timestamp) {
        long timestampLong = ~PtBytes.toLong(timestamp);
        Column col = new Column()
                .setName(CassandraKeyValueServices.makeCompositeBuffer(columnName, timestampLong));
        return new ColumnOrSuperColumn().setColumn(col);
    }
}
