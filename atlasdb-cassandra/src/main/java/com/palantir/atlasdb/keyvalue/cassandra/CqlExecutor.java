/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.logging.KvsProfilingLogger;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.common.base.Throwables;
import com.palantir.logsafe.Arg;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;

public class CqlExecutor {
    private QueryExecutor queryExecutor;

    public interface QueryExecutor {
        CqlResult execute(byte[] row, String query);
    }

    CqlExecutor(CassandraClientPool clientPool, ConsistencyLevel consistency) {
        this.queryExecutor = new QueryExecutorImpl(clientPool, consistency);
    }

    @VisibleForTesting
    CqlExecutor(QueryExecutor queryExecutor) {
        this.queryExecutor = queryExecutor;
    }

    /**
     * @param tableRef the table from which to select
     * @param row the row key
     * @param limit the maximum number of results to return.
     * @return up to <code>limit</code> cells that match the row name
     */
    List<CellWithTimestamp> getColumnsForRow(TableReference tableRef, byte[] row, int limit) {
        CqlQuery query = new CqlQuery(
                "SELECT column1, column2 FROM %s WHERE key = %s LIMIT %s;",
                quotedTableName(tableRef),
                key(row),
                limit(limit));
        return query.executeAndGetCells(row);
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
            byte[] column,
            long maxTimestampExclusive,
            int limit) {
        long invertedTimestamp = ~maxTimestampExclusive;
        CqlQuery query = new CqlQuery(
                "SELECT column1, column2 FROM %s WHERE key = %s AND column1 = %s AND column2 > %s LIMIT %s;",
                quotedTableName(tableRef),
                key(row),
                column1(column),
                column2(invertedTimestamp),
                limit(limit));
        return query.executeAndGetCells(row);
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
            byte[] previousColumn,
            int limit) {
        CqlQuery query = new CqlQuery(
                "SELECT column1, column2 FROM %s WHERE key = %s AND column1 > %s LIMIT %s;",
                quotedTableName(tableRef),
                key(row),
                column1(previousColumn),
                limit(limit));
        return query.executeAndGetCells(row);
    }

    private UnsafeArg<String> key(byte[] row) {
        return UnsafeArg.of("key", CassandraKeyValueServices.encodeAsHex(row));
    }

    private UnsafeArg<String> column1(byte[] column) {
        return UnsafeArg.of("column1", CassandraKeyValueServices.encodeAsHex(column));
    }

    private SafeArg<Long> column2(long invertedTimestamp) {
        return SafeArg.of("column2", invertedTimestamp);
    }

    private Arg<Long> limit(long limit) {
        return SafeArg.of("limit", limit);
    }

    private Arg<String> quotedTableName(TableReference tableRef) {
        String tableNameWithQuotes = "\"" + CassandraKeyValueService.internalTableName(tableRef) + "\"";
        return LoggingArgs.customTableName(tableRef, tableNameWithQuotes);
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

    private final class CqlQuery {
        private final String queryFormat;
        private final Arg<?>[] queryArgs;

        CqlQuery(String queryFormat, Arg<?>... args) {
            this.queryFormat = queryFormat;
            this.queryArgs = args;
        }

        public List<CellWithTimestamp> executeAndGetCells(byte[] row) {
            CqlResult cqlResult = KvsProfilingLogger.maybeLog(
                    () -> queryExecutor.execute(row, toString()),
                    this::logSlowResult,
                    this::logResultSize);
            return getCells(row, cqlResult);
        }

        private void logSlowResult(KvsProfilingLogger.LoggingFunction log, Stopwatch timer) {
            Object[] allArgs = new Object[queryArgs.length + 3];
            allArgs[0] = SafeArg.of("queryFormat", queryFormat);
            allArgs[1] = UnsafeArg.of("fullQuery", toString());
            allArgs[2] = LoggingArgs.durationMillis(timer);
            System.arraycopy(queryArgs, 0, allArgs, 3, queryArgs.length);

            log.log("A CQL query was slow: queryFormat = [{}], fullQuery = [{}], durationMillis = {}", allArgs);
        }

        private void logResultSize(KvsProfilingLogger.LoggingFunction log, CqlResult result) {
            log.log("and returned {} rows",
                    SafeArg.of("numRows", result.getRows().size()));
        }

        @Override
        public String toString() {
            return String.format(queryFormat, queryArgs);
        }
    }

    private static class QueryExecutorImpl implements QueryExecutor {
        private final CassandraClientPool clientPool;
        private final ConsistencyLevel consistency;

        QueryExecutorImpl(CassandraClientPool clientPool, ConsistencyLevel consistency) {
            this.clientPool = clientPool;
            this.consistency = consistency;
        }

        @Override
        public CqlResult execute(byte[] row, String query) {
            return executeQueryOnHost(query, getHostForRow(row));
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
    }
}
