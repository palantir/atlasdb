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
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.sweep.CellWithTimestamp;
import com.palantir.atlasdb.logging.KvsProfilingLogger;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.common.base.Throwables;
import com.palantir.logsafe.Arg;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;

public class CqlExecutor {
    private QueryExecutor queryExecutor;

    public interface QueryExecutor {
        CqlResult execute(byte[] rowHintForHostSelection, String query);
    }

    CqlExecutor(CassandraClientPool clientPool, ConsistencyLevel consistency) {
        this.queryExecutor = new QueryExecutorImpl(clientPool, consistency);
    }

    @VisibleForTesting
    CqlExecutor(QueryExecutor queryExecutor) {
        this.queryExecutor = queryExecutor;
    }

    /**
     * Returns a list of {@link CellWithTimestamp}s within the given {@code row}, starting at the given
     * {@code startRowInclusive}, potentially spanning across multiple rows.
     */
    public List<CellWithTimestamp> getTimestamps(
            TableReference tableRef,
            byte[] startRowInclusive,
            int limit) {
        CqlQuery query = new CqlQuery(
                "SELECT key, column1, column2 FROM %s WHERE token(key) >= token(%s) LIMIT %s;",
                quotedTableName(tableRef),
                key(startRowInclusive),
                limit(limit));
        return query.executeAndGetCells(startRowInclusive, this::getCellFromRow);
    }

    /**
     * Returns a list of {@link CellWithTimestamp}s within the given {@code row}, starting at the (column, timestamp)
     * pair represented by ({@code startColumnInclusive}, {@code startTimestampExclusive}).
     */
    public List<CellWithTimestamp> getTimestampsWithinRow(
            TableReference tableRef,
            byte[] row,
            byte[] startColumnInclusive,
            long startTimestampExclusive,
            int limit) {
        long invertedTimestamp = ~startTimestampExclusive;
        CqlQuery query = new CqlQuery(
                "SELECT column1, column2 FROM %s WHERE key = %s AND (column1, column2) > (%s, %s) LIMIT %s;",
                quotedTableName(tableRef),
                key(row),
                column1(startColumnInclusive),
                column2(invertedTimestamp),
                limit(limit));
        return query.executeAndGetCells(row, result -> getCellFromKeylessRow(result, row));
    }

    private CellWithTimestamp getCellFromKeylessRow(CqlRow row, byte[] key) {
        byte[] rowName = key;
        byte[] columnName = row.getColumns().get(0).getValue();
        long timestamp = extractTimestamp(row, 1);

        return CellWithTimestamp.of(Cell.create(rowName, columnName), timestamp);
    }

    private CellWithTimestamp getCellFromRow(CqlRow row) {
        byte[] rowName = row.getColumns().get(0).getValue();
        byte[] columnName = row.getColumns().get(1).getValue();
        long timestamp = extractTimestamp(row, 2);

        return CellWithTimestamp.of(Cell.create(rowName, columnName), timestamp);
    }

    private long extractTimestamp(CqlRow row, int columnIndex) {
        byte[] flippedTimestampAsBytes = row.getColumns().get(columnIndex).getValue();
        return ~PtBytes.toLong(flippedTimestampAsBytes);
    }

    private Arg<String> key(byte[] row) {
        return UnsafeArg.of("key", CassandraKeyValueServices.encodeAsHex(row));
    }

    private Arg<String> column1(byte[] column) {
        return UnsafeArg.of("column1", CassandraKeyValueServices.encodeAsHex(column));
    }

    private Arg<Long> column2(long invertedTimestamp) {
        return SafeArg.of("column2", invertedTimestamp);
    }

    private Arg<Long> limit(long limit) {
        return SafeArg.of("limit", limit);
    }

    private Arg<String> quotedTableName(TableReference tableRef) {
        String tableNameWithQuotes = "\"" + CassandraKeyValueServiceImpl.internalTableName(tableRef) + "\"";
        return LoggingArgs.customTableName(tableRef, tableNameWithQuotes);
    }

    private List<CellWithTimestamp> getCells(Function<CqlRow, CellWithTimestamp> cellTsExtractor, CqlResult cqlResult) {
        return cqlResult.getRows()
                .stream()
                .map(cellTsExtractor)
                .collect(Collectors.toList());
    }

    private final class CqlQuery {
        private final String queryFormat;
        private final Arg<?>[] queryArgs;

        CqlQuery(String queryFormat, Arg<?>... args) {
            this.queryFormat = queryFormat;
            this.queryArgs = args;
        }

        public List<CellWithTimestamp> executeAndGetCells(
                byte[] rowHintForHostSelection,
                Function<CqlRow, CellWithTimestamp> cellTsExtractor) {
            CqlResult cqlResult = KvsProfilingLogger.maybeLog(
                    () -> queryExecutor.execute(rowHintForHostSelection, toString()),
                    this::logSlowResult,
                    this::logResultSize);
            return getCells(cellTsExtractor, cqlResult);
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
        public CqlResult execute(byte[] rowHintForHostSelection, String query) {
            return executeQueryOnHost(query, getHostForRow(rowHintForHostSelection));
        }

        private InetSocketAddress getHostForRow(byte[] row) {
            return clientPool.getRandomHostForKey(row);
        }

        private CqlResult executeQueryOnHost(String query, InetSocketAddress host) {
            try {
                return clientPool.runWithRetryOnHost(host, createCqlFunction(query));
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

        private FunctionCheckedException<Cassandra.Client, CqlResult, TException> createCqlFunction(String query) {
            ByteBuffer queryBytes = ByteBuffer.wrap(query.getBytes(StandardCharsets.UTF_8));

            return new FunctionCheckedException<Cassandra.Client, CqlResult, TException>() {
                @Override
                public CqlResult apply(Cassandra.Client client) throws TException {
                    return client.execute_cql3_query(queryBytes, Compression.NONE, consistency);
                }

                @Override
                public String toString() {
                    return query;
                }
            };
        }
    }

}
