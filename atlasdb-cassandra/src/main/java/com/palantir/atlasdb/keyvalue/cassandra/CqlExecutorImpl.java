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

import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.sweep.CellWithTimestamp;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.common.base.Throwables;

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

    /**
     * Returns a list of {@link CellWithTimestamp}s within the given {@code row}, starting at the given
     * {@code startRowInclusive}, potentially spanning across multiple rows.
     */
    @Override
    public List<CellWithTimestamp> getTimestamps(
            TableReference tableRef,
            byte[] startRowInclusive,
            byte[] endRowInclusive,
            int limit) {
        String selQuery = "SELECT key, column1, column2 FROM %s"
                + " WHERE token(key) >= token(%s) AND token(key) <= token(%s) LIMIT %s;";
        CqlQuery query = new CqlQuery(
                selQuery,
                CqlQueryUtils.quotedTableName(tableRef),
                CqlQueryUtils.key(startRowInclusive),
                CqlQueryUtils.key(endRowInclusive),
                CqlQueryUtils.limit(limit));

        return executeAndGetCells(query, startRowInclusive, CqlQueryUtils::getCellFromRow);
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
                CqlQueryUtils.quotedTableName(tableRef),
                CqlQueryUtils.key(row),
                CqlQueryUtils.column1(startColumnInclusive),
                CqlQueryUtils.column2(invertedTimestamp),
                CqlQueryUtils.limit(limit));

        return executeAndGetCells(query, row,
                result -> CqlQueryUtils.getCellFromKeylessRow(result, row));
    }

    private List<CellWithTimestamp> executeAndGetCells(
            CqlQuery query,
            byte[] rowHintForHostSelection,
            Function<CqlRow, CellWithTimestamp> cellTsExtractor) {
        CqlResult cqlResult = queryExecutor.execute(query, rowHintForHostSelection);
        return CqlQueryUtils.getCells(cellTsExtractor, cqlResult);
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
