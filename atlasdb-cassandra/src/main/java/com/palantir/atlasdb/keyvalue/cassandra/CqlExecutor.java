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

import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.thrift.TException;

import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.common.base.Throwables;

public class CqlExecutor {
    private CassandraClientPool clientPool;
    private InetSocketAddress host;
    private ConsistencyLevel consistency;

    public CqlExecutor(CassandraClientPool clientPool, InetSocketAddress host, ConsistencyLevel consistency) {
        this.clientPool = clientPool;
        this.host = host;
        this.consistency = consistency;
    }

    public CqlResult getColAndTimestamp(TableReference tableRef, String row, int limit) {
        String query = String.format(
                "SELECT column1, column2 FROM %s WHERE key = %s LIMIT %s;",
                getTableName(tableRef),
                row,
                limit);
        return execute(query);
    }

    public CqlResult getColAndTimestampForColumnAndTimestamp(TableReference tableRef, String row, String columnNameStr, long timestamp, int limit) {
        String query = String.format(
                "SELECT column1, column2 FROM %s WHERE key = %s AND column1 = %s AND column2 > %s LIMIT %s;",
                getTableName(tableRef),
                row,
                columnNameStr,
                timestamp,
                limit);
        return execute(query);
    }

    public CqlResult getColAndTimestampForNextColumn(TableReference tableRef, String row, String columnNameStr, int limit) {
        String query = String.format(
                "SELECT column1, column2 FROM %s WHERE key = %s AND column1 > %s LIMIT %s;",
                getTableName(tableRef),
                row,
                columnNameStr,
                limit);
        return execute(query);
    }

    private CqlResult execute(String query) {
        ByteBuffer queryBytes = ByteBuffer.wrap(query.getBytes(StandardCharsets.UTF_8));
        return executeQuery(queryBytes);
    }

    private CqlResult executeQuery(ByteBuffer queryBytes) {
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
}
