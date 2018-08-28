/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.CASResult;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;

import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.qos.ThriftObjectSizeUtils;
import com.palantir.atlasdb.logging.KvsProfilingLogger;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.logsafe.SafeArg;

@SuppressWarnings({"all"}) // thrift variable names.
public class ProfilingCassandraClient implements AutoDelegate_CassandraClient {
    private final CassandraClient client;

    public ProfilingCassandraClient(CassandraClient client) {
        this.client = client;
    }

    @Override
    public CassandraClient delegate() {
        return this.client;
    }

    @Override
    public Map<ByteBuffer, List<ColumnOrSuperColumn>> multiget_slice(String kvsMethodName,
            TableReference tableRef,
            List<ByteBuffer> keys,
            SlicePredicate predicate,
            ConsistencyLevel consistency_level)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException {
        int numberOfKeys = keys.size();
        int numberOfColumns = predicate.slice_range.count;
        long startTime = System.currentTimeMillis();

        return KvsProfilingLogger.maybeLog(
                (KvsProfilingLogger.CallableCheckedException<Map<ByteBuffer, List<ColumnOrSuperColumn>>, TException>)
                        () -> client.multiget_slice(kvsMethodName, tableRef, keys, predicate, consistency_level),
                (logger, timer) -> logger.log("CassandraClient.multiget_slice({}, {}, {}, {}) at time {}, on kvs.{} took {} ms",
                        LoggingArgs.tableRef(tableRef),
                        LoggingArgs.rowCount(numberOfKeys),
                        LoggingArgs.columnCount(numberOfColumns),
                        SafeArg.of("consistency", consistency_level.toString()),
                        LoggingArgs.startTimeMillis(startTime),
                        SafeArg.of("kvsMethodName", kvsMethodName),
                        LoggingArgs.durationMillis(timer)),
                (logger, rowsToColumns) -> logger.log("and returned {} cells in {} rows with {} bytes",
                        LoggingArgs.cellCount(
                                rowsToColumns.values().stream().mapToInt(value -> value.size()).sum()),
                        LoggingArgs.rowCount(rowsToColumns.size()),
                        LoggingArgs.sizeInBytes(ThriftObjectSizeUtils.getApproximateSizeOfColsByKey(rowsToColumns))));
    }

    @Override
    public List<KeySlice> get_range_slices(String kvsMethodName,
            TableReference tableRef,
            SlicePredicate predicate,
            KeyRange range,
            ConsistencyLevel consistency_level)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException {
        int numberOfKeys = predicate.slice_range.count;
        int numberOfColumns = range.count;
        long startTime = System.currentTimeMillis();

        return KvsProfilingLogger.maybeLog(
                (KvsProfilingLogger.CallableCheckedException<List<KeySlice>, TException>)
                        () -> client.get_range_slices(kvsMethodName, tableRef, predicate, range, consistency_level),
                (logger, timer) -> logger.log("CassandraClient.get_range_slices({}, {}, {}, {}) at time {}, on kvs.{} took {} ms",
                        LoggingArgs.tableRef(tableRef),
                        LoggingArgs.rowCount(numberOfKeys),
                        LoggingArgs.columnCount(numberOfColumns),
                        SafeArg.of("consistency", consistency_level.toString()),
                        LoggingArgs.startTimeMillis(startTime),
                        SafeArg.of("kvsMethodName", kvsMethodName),
                        LoggingArgs.durationMillis(timer)),
                (logger, rows) -> logger.log("and returned {} rows with {} bytes",
                        LoggingArgs.rowCount(rows.size()),
                        LoggingArgs.sizeInBytes(ThriftObjectSizeUtils.getApproximateSizeOfKeySlices(rows))));
    }

    @Override
    public void remove(String kvsMethodName, TableReference tableRef, byte[] row, long timestamp,
            ConsistencyLevel consistency_level)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException {
        long startTime = System.currentTimeMillis();
        KvsProfilingLogger.maybeLog(
                (KvsProfilingLogger.CallableCheckedException<Void, TException>)
                        () -> {
                             client.remove(kvsMethodName, tableRef, row, timestamp, consistency_level);
                             return null;
                        },
                (logger, timer) -> logger.log("CassandraClient.remove({}, {}, {}, {}) at time {}, on kvs.{} took {} ms",
                        LoggingArgs.tableRef(tableRef),
                        SafeArg.of("timestamp", timestamp),
                        SafeArg.of("consistency", consistency_level.toString()),
                        LoggingArgs.startTimeMillis(startTime),
                        SafeArg.of("kvsMethodName", kvsMethodName),
                        LoggingArgs.durationMillis(timer)));
    }

    @Override
    public void batch_mutate(String kvsMethodName,
            Map<ByteBuffer, Map<String, List<Mutation>>> mutation_map,
            ConsistencyLevel consistency_level)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException {
        long startTime = System.currentTimeMillis();

        KvsProfilingLogger.maybeLog(
                (KvsProfilingLogger.CallableCheckedException<Void, TException>)
                        () -> {
                            client.batch_mutate(kvsMethodName, mutation_map, consistency_level);
                            return null;
                        },
                (logger, timer) -> {
                    logger.log("CassandraClient.batch_mutate(");
                    ThriftObjectSizeUtils.getSizeOfMutationPerTable(mutation_map).forEach((tableName, size) -> {
                        logger.log("{} -> {}",
                                LoggingArgs.safeInternalTableNameOrPlaceholder(tableName),
                                LoggingArgs.sizeInBytes(size));
                    });
                    logger.log(") with consistency {} at time {}, on kvs.{} took {} ms",
                            SafeArg.of("consistency", consistency_level.toString()),
                            LoggingArgs.startTimeMillis(startTime),
                            SafeArg.of("kvsMethodName", kvsMethodName),
                            LoggingArgs.durationMillis(timer));
                });
    }

    @Override
    public ColumnOrSuperColumn get(TableReference tableReference,
            ByteBuffer key,
            byte[] column,
            ConsistencyLevel consistency_level)
            throws InvalidRequestException, NotFoundException, UnavailableException, TimedOutException, TException {
        long startTime = System.currentTimeMillis();

        return KvsProfilingLogger.maybeLog(
                (KvsProfilingLogger.CallableCheckedException<ColumnOrSuperColumn, TException>)
                        () -> client.get(tableReference, key, column, consistency_level),
                (logger, timer) -> logger.log("CassandraClient.get({}, {}) at time {} took {} ms",
                        LoggingArgs.tableRef(tableReference),
                        SafeArg.of("consistency", consistency_level.toString()),
                        LoggingArgs.startTimeMillis(startTime),
                        LoggingArgs.durationMillis(timer)));
    }

    @Override
    public CASResult cas(TableReference tableReference,
            ByteBuffer key,
            List<Column> expected,
            List<Column> updates,
            ConsistencyLevel serial_consistency_level,
            ConsistencyLevel commit_consistency_level)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException {
        long startTime = System.currentTimeMillis();

        return KvsProfilingLogger.maybeLog(
                (KvsProfilingLogger.CallableCheckedException<CASResult, TException>)
                        () -> client.cas(tableReference, key, expected, updates, serial_consistency_level,
                                commit_consistency_level),
                (logger, timer) -> logger.log("CassandraClient.cas({}) at time {} took {} ms",
                        LoggingArgs.tableRef(tableReference),
                        LoggingArgs.startTimeMillis(startTime),
                        LoggingArgs.durationMillis(timer)));
    }

    @Override
    public CqlResult execute_cql3_query(CqlQuery cqlQuery, Compression compression, ConsistencyLevel consistency)
            throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException,
            TException {
        long startTime = System.currentTimeMillis();

        return KvsProfilingLogger.maybeLog(
                (KvsProfilingLogger.CallableCheckedException<CqlResult, TException>)
                        () -> client.execute_cql3_query(cqlQuery, compression, consistency),
                (logger, timer) -> cqlQuery.logSlowResult(logger, timer),
                (logger, cqlResult) -> {
                    if (cqlResult.getRows() == null) {
                        // different from an empty list
                        logger.log("and returned null rows. The query was started at time {}",
                                LoggingArgs.startTimeMillis(startTime));
                    } else {
                        logger.log("and returned {} rows. The query was started at time {}",
                                SafeArg.of("numRows", cqlResult.getRows().size()),
                                LoggingArgs.startTimeMillis(startTime));
                    }
                });
    }
}
