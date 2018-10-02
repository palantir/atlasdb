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
 */
package com.palantir.atlasdb.keyvalue.cassandra;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.thrift.TException;

import com.google.common.collect.Iterables;
import com.google.common.primitives.Longs;
import com.palantir.logsafe.SafeArg;

public final class SchemaMutationLockTestTools {
    private final CassandraClientPool clientPool;
    private final UniqueSchemaMutationLockTable lockTable;

    public SchemaMutationLockTestTools(CassandraClientPool clientPool, UniqueSchemaMutationLockTable lockTable) {
        this.clientPool = clientPool;
        this.lockTable = lockTable;
    }

    public CqlResult setLegacyClearedLocksTableValue() throws TException {
        String lockValue = CassandraKeyValueServices.encodeAsHex(Longs.toByteArray(Long.MAX_VALUE));
        return setLocksTableValueInternal(lockValue);
    }

    public CqlResult setLocksTableValue(long lockId, int heartbeatCount) throws TException {
        String lockValue = getHexEncodedBytes(SchemaMutationLock.lockValueFromIdAndHeartbeat(lockId, heartbeatCount));
        return setLocksTableValueInternal(lockValue);
    }

    public CqlResult truncateLocksTable() throws TException {
        return clientPool.run(client -> {
            CqlQuery truncateQuery = CqlQuery.builder()
                    .safeQueryFormat("TRUNCATE \"%s\";")
                    .addArgs(SafeArg.of("lockTable", lockTable.getOnlyTable().getQualifiedName()))
                    .build();
            return runCqlQuery(truncateQuery, client, ConsistencyLevel.ALL);
        });
    }

    public CqlResult readLocksTable() throws TException {
        return clientPool.run(client -> {
            String lockRowName = getHexEncodedBytes(CassandraConstants.GLOBAL_DDL_LOCK_ROW_NAME);
            String lockColName = getHexEncodedBytes(CassandraConstants.GLOBAL_DDL_LOCK_COLUMN_NAME);
            CqlQuery selectCql = CqlQuery.builder()
                    .safeQueryFormat("SELECT \"value\" FROM \"%s\" WHERE key = %s AND column1 = %s AND column2 = -1;")
                    .addArgs(
                            SafeArg.of("lockTable", lockTable.getOnlyTable().getQualifiedName()),
                            SafeArg.of("lockRow", lockRowName),
                            SafeArg.of("lockColumn", lockColName))
                    .build();
            return runCqlQuery(selectCql, client, ConsistencyLevel.LOCAL_QUORUM);
        });
    }

    public long readLockIdFromLocksTable() throws TException {
        CqlResult result = readLocksTable();
        Column resultColumn = getColumnFromCqlResult(result);
        return SchemaMutationLock.getLockIdFromColumn(resultColumn);
    }

    public long readHeartbeatCountFromLocksTable() throws TException {
        CqlResult result = readLocksTable();
        Column resultColumn = getColumnFromCqlResult(result);
        return SchemaMutationLock.getHeartbeatCountFromColumn(resultColumn);
    }

    private CqlResult setLocksTableValueInternal(String hexLockValue) throws TException {
        return clientPool.run(client -> {
            String lockRowName = getHexEncodedBytes(CassandraConstants.GLOBAL_DDL_LOCK_ROW_NAME);
            String lockColName = getHexEncodedBytes(CassandraConstants.GLOBAL_DDL_LOCK_COLUMN_NAME);
            String lockTableName = lockTable.getOnlyTable().getQualifiedName();
            CqlQuery updateCql = CqlQuery.builder()
                    .safeQueryFormat("UPDATE \"%s\" SET value = %s WHERE key = %s AND column1 = %s AND column2 = -1;")
                    .addArgs(
                            SafeArg.of("lockTable", lockTableName),
                            SafeArg.of("hexLockValue", hexLockValue),
                            SafeArg.of("lockRow", lockRowName),
                            SafeArg.of("lockCol", lockColName))
                    .build();
            return runCqlQuery(updateCql, client, ConsistencyLevel.EACH_QUORUM);
        });
    }

    private static Column getColumnFromCqlResult(CqlResult result) {
        List<CqlRow> resultRows = result.getRows();
        List<Column> resultColumns = Iterables.getOnlyElement(resultRows).getColumns();
        return Iterables.getOnlyElement(resultColumns);
    }

    private static String getHexEncodedBytes(String str) {
        return CassandraKeyValueServices.encodeAsHex(str.getBytes(StandardCharsets.UTF_8));
    }

    private static CqlResult runCqlQuery(CqlQuery cqlQuery, CassandraClient client, ConsistencyLevel consistency)
            throws TException {
        return client.execute_cql3_query(cqlQuery, Compression.NONE, consistency);
    }
}
