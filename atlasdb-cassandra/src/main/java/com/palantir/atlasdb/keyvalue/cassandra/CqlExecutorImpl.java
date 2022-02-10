/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.cassandra;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.RetryLimitReachedException;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.pool.DcAwareHost;
import com.palantir.atlasdb.keyvalue.cassandra.sweep.CellWithTimestamp;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.common.annotation.Output;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.common.base.Throwables;
import com.palantir.logsafe.Arg;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlPreparedResult;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.thrift.TException;

public class CqlExecutorImpl implements CqlExecutor {
    private final QueryExecutor queryExecutor;
    private static final SafeLogger log = SafeLoggerFactory.get(CqlExecutorImpl.class);

    public interface QueryExecutor {
        CqlResult execute(CqlQuery cqlQuery, byte[] rowHintForHostSelection);

        CqlPreparedResult prepare(ByteBuffer query, byte[] rowHintForHostSelection, Compression compression);

        CqlResult executePrepared(int queryId, List<ByteBuffer> values);
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
            int limit,
            ExecutorService executor,
            Integer executorThreads) {
        String preparedSelQuery = String.format(
                "SELECT key, column1, column2 FROM %s WHERE key = ? LIMIT %d;",
                quotedTableName(tableRef).getValue(), limit);
        ByteBuffer queryBytes = ByteBuffer.wrap(preparedSelQuery.getBytes(StandardCharsets.UTF_8));

        CqlPreparedResult preparedResult = queryExecutor.prepare(queryBytes, rowsAscending.get(0), Compression.NONE);
        int queryId = preparedResult.getItemId();

        List<CellWithTimestamp> result = new ArrayList<>();

        List<Future<CqlResult>> futures = new ArrayList<>(rowsAscending.size());
        for (int i = 0; i < rowsAscending.size(); i++) {
            futures.add(null);
        }
        AtomicInteger nextRowToQuery = new AtomicInteger(0);
        for (int i = 0; i < executorThreads; i++) {
            scheduleSweepRowTask(
                    futures, queryId, nextRowToQuery.getAndIncrement(), nextRowToQuery, rowsAscending, executor);
        }

        try {
            for (int rowIndex = 0; result.size() < limit && rowIndex < rowsAscending.size(); rowIndex++) {
                Future<CqlResult> future = futures.get(rowIndex);
                CqlResult cqlResult = future.get();
                result.addAll(CqlExecutorImpl.getCells(CqlExecutorImpl::getCellFromRow, cqlResult));

                if (result.size() > limit) {
                    cancelFutures(futures.subList(rowIndex, futures.size()));
                    break;
                }
            }
        } catch (InterruptedException e) {
            throw Throwables.throwUncheckedException(e);
        } catch (ExecutionException e) {
            throw Throwables.throwUncheckedException(e.getCause());
        }

        return result;
    }

    private void scheduleSweepRowTask(
            @Output List<Future<CqlResult>> futures,
            int queryId,
            int rowIndex,
            AtomicInteger nextRowToQuery,
            List<byte[]> rows,
            ExecutorService executor) {
        if (rowIndex >= rows.size()) {
            return;
        }

        byte[] row = rows.get(rowIndex);

        Callable<CqlResult> task = () -> {
            CqlResult cqlResult = queryExecutor.executePrepared(queryId, ImmutableList.of(ByteBuffer.wrap(row)));
            if (!Thread.interrupted()) {
                scheduleSweepRowTask(
                        futures, queryId, nextRowToQuery.getAndIncrement(), nextRowToQuery, rows, executor);
            }
            return cqlResult;
        };

        try {
            Future<CqlResult> future = executor.submit(task);
            futures.set(rowIndex, future);
        } catch (RejectedExecutionException e) {
            // RejectedExecutionException are expected.
            // The executor is shutdown when we already fetched all the values we were interested
            // for the current iteration.
            log.trace("Rejecting row {} because executor is closed", UnsafeArg.of("row", rows.get(rowIndex)), e);
        }
    }

    private void cancelFutures(List<Future<CqlResult>> futures) {
        futures.stream().filter(Objects::nonNull).forEach(f -> f.cancel(true));
    }

    /**
     * Returns a list of {@link CellWithTimestamp}s within the given {@code row}, starting at the (column, timestamp)
     * pair represented by ({@code startColumnInclusive}, {@code startTimestampExclusive}).
     */
    @Override
    public List<CellWithTimestamp> getTimestampsWithinRow(
            TableReference tableRef, byte[] row, byte[] startColumnInclusive, long startTimestampExclusive, int limit) {
        long invertedTimestamp = ~startTimestampExclusive;
        String selQuery = "SELECT column1, column2 FROM %s WHERE key = %s AND (column1, column2) > (%s, %s) LIMIT %s;";
        CqlQuery query = CqlQuery.builder()
                .safeQueryFormat(selQuery)
                .addArgs(
                        quotedTableName(tableRef),
                        key(row),
                        column1(startColumnInclusive),
                        column2(invertedTimestamp),
                        limit(limit))
                .build();

        return executeAndGetCells(query, row, result -> CqlExecutorImpl.getCellFromKeylessRow(result, row));
    }

    private List<CellWithTimestamp> executeAndGetCells(
            CqlQuery query, byte[] rowHintForHostSelection, Function<CqlRow, CellWithTimestamp> cellTsExtractor) {
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

    private static List<CellWithTimestamp> getCells(
            Function<CqlRow, CellWithTimestamp> cellTsExtractor, CqlResult cqlResult) {
        return cqlResult.getRows().stream().map(cellTsExtractor).collect(Collectors.toList());
    }

    private static class QueryExecutorImpl implements QueryExecutor {
        private final CassandraClientPool clientPool;
        private final ConsistencyLevel consistency;

        private Map<Integer, DcAwareHost> hostsPerPreparedQuery;

        QueryExecutorImpl(CassandraClientPool clientPool, ConsistencyLevel consistency) {
            this.clientPool = clientPool;
            this.consistency = consistency;
            this.hostsPerPreparedQuery = new HashMap<>();
        }

        @Override
        public CqlResult execute(CqlQuery cqlQuery, byte[] rowHintForHostSelection) {
            return executeQueryOnHost(cqlQuery, getHostForRow(rowHintForHostSelection));
        }

        @Override
        public CqlPreparedResult prepare(ByteBuffer query, byte[] rowHintForHostSelection, Compression compression) {
            FunctionCheckedException<CassandraClient, CqlPreparedResult, TException> prepareFunction =
                    client -> client.prepare_cql3_query(query, compression);

            try {
                DcAwareHost hostForRow = getHostForRow(rowHintForHostSelection);
                CqlPreparedResult preparedResult = clientPool.runWithRetryOnHost(hostForRow, prepareFunction);
                hostsPerPreparedQuery.put(preparedResult.getItemId(), hostForRow);
                return preparedResult;
            } catch (TException e) {
                throw Throwables.throwUncheckedException(e);
            }
        }

        @Override
        public CqlResult executePrepared(int queryId, List<ByteBuffer> values) {
            FunctionCheckedException<CassandraClient, CqlResult, TException> cqlFunction =
                    client -> client.execute_prepared_cql3_query(queryId, values, consistency);

            DcAwareHost host = hostsPerPreparedQuery.getOrDefault(
                    queryId, getHostForRow(values.get(0).array()));

            return executeFunctionOnHost(cqlFunction, host);
        }

        private DcAwareHost getHostForRow(byte[] row) {
            return clientPool.getRandomHostForKey(row);
        }

        private CqlResult executeQueryOnHost(CqlQuery cqlQuery, DcAwareHost host) {
            return executeFunctionOnHost(createCqlFunction(cqlQuery), host);
        }

        private CqlResult executeFunctionOnHost(
                FunctionCheckedException<CassandraClient, CqlResult, TException> cqlFunction, DcAwareHost host) {
            try {
                return clientPool.runWithRetryOnHost(host, cqlFunction);
            } catch (RetryLimitReachedException e) {
                if (consistency.equals(ConsistencyLevel.ALL)) {
                    throw CassandraUtils.wrapInIceForDeleteOrRethrow(e);
                }
                throw e;
            } catch (TException e) {
                throw Throwables.throwUncheckedException(e);
            }
        }

        private FunctionCheckedException<CassandraClient, CqlResult, TException> createCqlFunction(CqlQuery cqlQuery) {
            return new FunctionCheckedException<>() {
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
