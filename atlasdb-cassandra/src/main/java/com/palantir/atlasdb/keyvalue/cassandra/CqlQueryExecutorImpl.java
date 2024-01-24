/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

import com.palantir.atlasdb.keyvalue.api.RetryLimitReachedException;
import com.palantir.atlasdb.keyvalue.cassandra.pool.CassandraServer;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.common.base.Throwables;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlPreparedResult;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.thrift.TException;

public class CqlQueryExecutorImpl implements CqlQueryExecutor {
    private final CassandraClientPool clientPool;
    private final ConsistencyLevel consistency;

    private final Map<Integer, CassandraServer> hostsPerPreparedQuery;

    CqlQueryExecutorImpl(CassandraClientPool clientPool, ConsistencyLevel consistency) {
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
            CassandraServer hostForRow = getHostForRow(rowHintForHostSelection);
            CqlPreparedResult preparedResult = clientPool.runWithRetryOnServer(hostForRow, prepareFunction);
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

        CassandraServer host = hostsPerPreparedQuery.getOrDefault(
                queryId, getHostForRow(values.get(0).array()));

        return executeFunctionOnHost(cqlFunction, host);
    }

    private CassandraServer getHostForRow(byte[] row) {
        return clientPool.getRandomServerForKey(row);
    }

    private CqlResult executeQueryOnHost(CqlQuery cqlQuery, CassandraServer cassandraServer) {
        return executeFunctionOnHost(createCqlFunction(cqlQuery), cassandraServer);
    }

    private CqlResult executeFunctionOnHost(
            FunctionCheckedException<CassandraClient, CqlResult, TException> cqlFunction,
            CassandraServer cassandraServer) {
        try {
            return clientPool.runWithRetryOnServer(cassandraServer, cqlFunction);
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
