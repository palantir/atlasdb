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
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.sweep.CellWithTimestamp;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.common.base.Throwables;
import com.palantir.logsafe.Arg;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;

public class CqlExecutorImpl implements CqlExecutor {
    private QueryExecutor queryExecutor;

    public interface QueryExecutor {
        CqlResult execute(CqlQuery cqlQuery, byte[] rowHintForHostSelection);
    }

    CqlExecutorImpl(CassandraClientPool clientPool, ConsistencyLevel consistency) {
        this.queryExecutor = new QueryExecutorImpl(clientPool, consistency);
    }

    @VisibleForTesting
    CqlExecutorImpl(QueryExecutor queryExecutor) {
        this.queryExecutor = queryExecutor;
    }

    @Override
    public List<CellWithTimestamp> getTimestamps(
            TableReference tableRef,
            List<byte[]> rowsAscending,
            int limit) {
        String selQuery = "SELECT key, column1, column2 FROM %s WHERE key IN (%s) LIMIT %s;";
        CqlQuery query = new CqlQuery(selQuery, quotedTableName(tableRef), keys(rowsAscending), limit(limit));
        return executeAndGetCells(query, rowsAscending.get(0), CqlExecutorImpl::getCellFromRow);
    }

    private Arg<String> keys(List<byte[]> rowsAscending) {
        return UnsafeArg.of("keys",
                rowsAscending.stream().map(CqlExecutorImpl::getKey)
                        .collect(Collectors.joining(",")));
    }

    /**
     * Returns a list of {@link CellWithTimestamp}s within the given {@code row}, starting at the (column, timestamp)
     * pair represented by ({@code startColumnInclusive}, {@code startTimestampExclusive}).
     */
    @Override
    public List<CellWithTimestamp> getTimestampsWithinRow(
            TableReference tableRef,
            byte[] row,
            byte[] startColumnInclusive,
            long startTimestampExclusive,
            int limit) {
        long invertedTimestamp = ~startTimestampExclusive;
        String selQuery = "SELECT column1, column2 FROM %s WHERE key = %s AND (column1, column2) > (%s, %s) LIMIT %s;";
        CqlQuery query = new CqlQuery(
                selQuery,
                quotedTableName(tableRef),
                key(row),
                column1(startColumnInclusive),
                column2(invertedTimestamp),
                limit(limit));

        return executeAndGetCells(query, row,
                result -> CqlExecutorImpl.getCellFromKeylessRow(result, row));
    }

    private List<CellWithTimestamp> executeAndGetCells(
            CqlQuery query,
            byte[] rowHintForHostSelection,
            Function<CqlRow, CellWithTimestamp> cellTsExtractor) {
        CqlResult cqlResult = queryExecutor.execute(query, rowHintForHostSelection);
        return CqlExecutorImpl.getCells(cellTsExtractor, cqlResult);
    }

    private static CellWithTimestamp getCellFromKeylessRow(CqlRow row, byte[] key) {
        byte[] rowName = key;
        byte[] columnName = row.getColumns().get(0).getValue();
        long timestamp = extractTimestamp(row, 1);

        return CellWithTimestamp.of(Cell.create(rowName, columnName), timestamp);
    }

    private static CellWithTimestamp getCellFromRow(CqlRow row) {
        byte[] rowName = row.getColumns().get(0).getValue();
        byte[] columnName = row.getColumns().get(1).getValue();
        long timestamp = extractTimestamp(row, 2);

        return CellWithTimestamp.of(Cell.create(rowName, columnName), timestamp);
    }

    private static long extractTimestamp(CqlRow row, int columnIndex) {
        byte[] flippedTimestampAsBytes = row.getColumns().get(columnIndex).getValue();
        return ~PtBytes.toLong(flippedTimestampAsBytes);
    }

    private static Arg<String> key(byte[] row) {
        return UnsafeArg.of("key", getKey(row));
    }

    private static String getKey(byte[] row) {
        return CassandraKeyValueServices.encodeAsHex(row);
    }

    private static Arg<String> column1(byte[] column) {
        return UnsafeArg.of("column1", CassandraKeyValueServices.encodeAsHex(column));
    }

    private static Arg<Long> column2(long invertedTimestamp) {
        return SafeArg.of("column2", invertedTimestamp);
    }

    private static Arg<Long> limit(long limit) {
        return SafeArg.of("limit", limit);
    }

    private static Arg<String> quotedTableName(TableReference tableRef) {
        String tableNameWithQuotes = "\"" + CassandraKeyValueServiceImpl.internalTableName(tableRef) + "\"";
        return LoggingArgs.customTableName(tableRef, tableNameWithQuotes);
    }

    private static List<CellWithTimestamp> getCells(Function<CqlRow,
            CellWithTimestamp> cellTsExtractor,
            CqlResult cqlResult) {
        return cqlResult.getRows()
                .stream()
                .map(cellTsExtractor)
                .collect(Collectors.toList());
    }

    private static class QueryExecutorImpl implements QueryExecutor {
        private final CassandraClientPool clientPool;
        private final ConsistencyLevel consistency;

        QueryExecutorImpl(CassandraClientPool clientPool, ConsistencyLevel consistency) {
            this.clientPool = clientPool;
            this.consistency = consistency;
        }

        @Override
        public CqlResult execute(CqlQuery cqlQuery, byte[] rowHintForHostSelection) {
            return executeQueryOnHost(cqlQuery, getHostForRow(rowHintForHostSelection));
        }

        private InetSocketAddress getHostForRow(byte[] row) {
            return clientPool.getRandomHostForKey(row);
        }

        private CqlResult executeQueryOnHost(CqlQuery cqlQuery, InetSocketAddress host) {
            try {
                return clientPool.runWithRetryOnHost(host, createCqlFunction(cqlQuery));
            } catch (UnavailableException e) {
                throw wrapIfConsistencyAll(e);
            } catch (TException e) {
                throw Throwables.throwUncheckedException(e);
            }
        }

        private RuntimeException wrapIfConsistencyAll(UnavailableException ex) {
            if (consistency.equals(ConsistencyLevel.ALL)) {
                throw new InsufficientConsistencyException("This operation requires all Cassandra"
                        + " nodes to be up and available.", ex);
            } else {
                throw Throwables.throwUncheckedException(ex);
            }
        }

        private FunctionCheckedException<CassandraClient, CqlResult, TException> createCqlFunction(CqlQuery cqlQuery) {
            return new FunctionCheckedException<CassandraClient, CqlResult, TException>() {
                @Override
                public CqlResult apply(CassandraClient client) throws TException {
                    return client.execute_cql3_query(cqlQuery, Compression.NONE, consistency);
                }

                @Override
                public String toString() {
                    return cqlQuery.toString();
                }
            };
        }
    }

}
