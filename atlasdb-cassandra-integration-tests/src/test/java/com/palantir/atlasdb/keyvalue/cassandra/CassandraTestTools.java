/**
 * Copyright 2015 Palantir Technologies
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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.thrift.TException;
import org.joda.time.Duration;

import com.jayway.awaitility.Awaitility;
import com.jayway.awaitility.core.ConditionTimeoutException;

/**
 * Utilities for ETE tests
 * Created by aloro on 12/04/2016.
 */
public final class CassandraTestTools {
    private CassandraTestTools() {
        // Empty constructor for utility class
    }

    public static void waitTillServiceIsUp(String host, int port, Duration timeout) {
        try {
            Awaitility.await()
                    .pollInterval(50, TimeUnit.MILLISECONDS)
                    .atMost(timeout.getMillis(), TimeUnit.MILLISECONDS)
                    .until(isPortListening(host, port));
        } catch (ConditionTimeoutException e) {
            throw new IllegalStateException("Timeout for port " + port + " on host " + host + ".");
        }
    }

    public static Future async(ExecutorService executorService, Runnable callable) {
        return executorService.submit(callable);
    }

    public static void assertThatFutureDidNotSucceedYet(Future future) throws InterruptedException {
        if (future.isDone()) {
            try {
                future.get();
                throw new AssertionError("Future task should have failed but finished successfully");
            } catch (ExecutionException e) {
                // if execution is done, we expect it to have failed
            }
        }
    }

    public static CqlResult setLocksTableValue(
            CassandraClientPool clientPool,
            UniqueSchemaMutationLockTable lockTable,
            String hexLockValue,
            ConsistencyLevel consistency) throws TException {
        return clientPool.run(client -> {
            String updateCql = "UPDATE \"%s\" SET value = %s WHERE key = %s AND column1 = %s AND column2 = -1;";
            String lockRowName = getHexEncodedBytes(CassandraConstants.GLOBAL_DDL_LOCK_ROW_NAME);
            String lockColName = getHexEncodedBytes(CassandraConstants.GLOBAL_DDL_LOCK_COLUMN_NAME);
            String lockTableName = lockTable.getOnlyTable().getQualifiedName();
            updateCql = String.format(updateCql, lockTableName, hexLockValue, lockRowName, lockColName);
            return runCqlQuery(updateCql, client, consistency);
        });
    }

    public static CqlResult truncateLocksTable(CassandraClientPool clientPool, UniqueSchemaMutationLockTable lockTable)
            throws TException {
        return clientPool.run(client -> {
            String truncateCql = String.format("TRUNCATE \"%s\";", lockTable.getOnlyTable().getQualifiedName());
            return runCqlQuery(truncateCql, client, ConsistencyLevel.ALL);
        });
    }

    public static CqlResult readLocksTable(CassandraClientPool clientPool, UniqueSchemaMutationLockTable lockTable)
            throws TException {
        return clientPool.run(client -> {
            String selectCql = "SELECT \"value\" FROM \"%s\" WHERE key = %s AND column1 = %s AND column2 = -1;";
            String lockRowName = getHexEncodedBytes(CassandraConstants.GLOBAL_DDL_LOCK_ROW_NAME);
            String lockColName = getHexEncodedBytes(CassandraConstants.GLOBAL_DDL_LOCK_COLUMN_NAME);
            selectCql = String.format(selectCql, lockTable.getOnlyTable().getQualifiedName(), lockRowName, lockColName);
            return runCqlQuery(selectCql, client, ConsistencyLevel.LOCAL_QUORUM);
        });
    }

    public static String getHexEncodedBytes(String str) {
        return CassandraKeyValueServices.encodeAsHex(str.getBytes(StandardCharsets.UTF_8));
    }

    public static long readLockIdFromLocksTable(CassandraClientPool clientPool, UniqueSchemaMutationLockTable lockTable)
            throws TException {
        CqlResult result = CassandraTestTools.readLocksTable(clientPool, lockTable);
        Column resultColumn = getColumnFromCqlResult(result);
        return SchemaMutationLock.getLockIdFromColumn(resultColumn);
    }

    public static long readHeartbeatCountFromLocksTable(CassandraClientPool clientPool,
            UniqueSchemaMutationLockTable lockTable) throws TException {
        CqlResult result = CassandraTestTools.readLocksTable(clientPool, lockTable);
        Column resultColumn = getColumnFromCqlResult(result);
        return SchemaMutationLock.getHeartbeatCountFromColumn(resultColumn);
    }

    private static Column getColumnFromCqlResult(CqlResult result) {
        List<CqlRow> resultRows = result.getRows();
        assertEquals(1, resultRows.size());
        List<Column> resultColumns = resultRows.get(0).getColumns();
        assertEquals(1, resultColumns.size());
        return resultColumns.get(0);
    }

    private static CqlResult runCqlQuery(String query, Client client, ConsistencyLevel consistency) throws TException {
        ByteBuffer queryBuffer = ByteBuffer.wrap(query.getBytes(StandardCharsets.UTF_8));
        return client.execute_cql3_query(queryBuffer, Compression.NONE, consistency);
    }

    private static Callable<Boolean> isPortListening(String host, int port) {
        return () -> {
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(host, port), 500);
                return true;
            } catch (IOException e) {
                return false;
            }
        };
    }
}
