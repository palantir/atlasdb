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
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.CASResult;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
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

import com.google.common.base.Stopwatch;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.AbstractKeyValueService;
import com.palantir.atlasdb.logging.KvsProfilingLogger;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.tracing.CloseableTrace;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;

@SuppressWarnings({"all"}) // thrift variable names.
public class CassandraClientImpl implements CassandraClient {
    private static final String SERVICE_NAME = "cassandra-thrift-client";

    private final Cassandra.Client client;

    public CassandraClientImpl(Cassandra.Client client) {
        this.client = client;
    }

    @Override
    public Cassandra.Client rawClient() {
        return client;
    }

    @Override
    public Map<ByteBuffer, List<ColumnOrSuperColumn>> multiget_slice(
            String kvsMethodName,
            TableReference tableRef,
            List<ByteBuffer> keys,
            SlicePredicate predicate,
            ConsistencyLevel consistency_level)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException {
        int numberOfKeys = keys.size();
        int numberOfColumns = predicate.slice_range.count;

        try (CloseableTrace trace = startLocalTrace(
                "client.multiget_slice(table {}, number of keys {}, number of columns {}, consistency {}) on kvs.{}",
                LoggingArgs.safeTableOrPlaceholder(tableRef),
                numberOfKeys, numberOfColumns, consistency_level, kvsMethodName)) {
            ColumnParent colFam = getColumnParent(tableRef);

            return KvsProfilingLogger.maybeLog(
                    (KvsProfilingLogger.CallableCheckedException<Map<ByteBuffer, List<ColumnOrSuperColumn>>, TException>)
                            () -> client.multiget_slice(keys, colFam, predicate, consistency_level),
                    (logger, timer) -> logger.log("client.multiget_slice({}, {}, {}, {}) on kvs.{}",
                            LoggingArgs.tableRef(tableRef),
                            SafeArg.of("number of keys", numberOfKeys),
                            SafeArg.of("number of columns", numberOfColumns),
                            SafeArg.of("consistency", consistency_level.toString()),
                            SafeArg.of("kvsMethodName", kvsMethodName)));
        }
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

        try (CloseableTrace trace = startLocalTrace(
                "client.get_range_slices(table {}, number of keys {}, number of columns {}, consistency {}) on kvs.{}",
                LoggingArgs.safeTableOrPlaceholder(tableRef),
                numberOfKeys, numberOfColumns, consistency_level, kvsMethodName)) {
            ColumnParent colFam = getColumnParent(tableRef);

            return KvsProfilingLogger.maybeLog(
                    (KvsProfilingLogger.CallableCheckedException<List<KeySlice>, TException>)
                            () -> client.get_range_slices(colFam, predicate, range, consistency_level),
                    (logger, timer) -> logger.log("client.get_range_slices({}, {}, {}, {}) on kvs.{}",
                            LoggingArgs.tableRef(tableRef),
                            SafeArg.of("number of keys", numberOfKeys),
                            SafeArg.of("number of columns", numberOfColumns),
                            SafeArg.of("consistency", consistency_level.toString()),
                            SafeArg.of("kvsMethodName", kvsMethodName)));
        }
    }

    @Override
    public void batch_mutate(String kvsMethodName,
            Map<ByteBuffer, Map<String, List<Mutation>>> mutation_map,
            ConsistencyLevel consistency_level)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException {
        int numberOfRowsMutated = mutation_map.size();

        try (CloseableTrace trace = startLocalTrace("client.batch_mutate(number of rows mutated {}, consistency {})"
                        + " on kvs.{}",
                numberOfRowsMutated, consistency_level, kvsMethodName)) {
            KvsProfilingLogger.maybeLog(
                    (KvsProfilingLogger.CallableCheckedException<Void, TException>)
                            () -> {
                                client.batch_mutate(mutation_map, consistency_level);
                                return null;
                            },
                    (logger, timer) -> logger.log("client.batch_mutate({}, {}) on kvs.{}",
                            SafeArg.of("number of mutations", numberOfRowsMutated),
                            SafeArg.of("consistency", consistency_level.toString()),
                            SafeArg.of("kvsMethodName", kvsMethodName)));
        }
    }

    @Override
    public ColumnOrSuperColumn get(TableReference tableReference,
            ByteBuffer key,
            byte[] column,
            ConsistencyLevel consistency_level)
            throws InvalidRequestException, NotFoundException, UnavailableException, TimedOutException, TException {
        ColumnPath columnPath = new ColumnPath(tableReference.getQualifiedName());
        columnPath.setColumn(column);

        try (CloseableTrace trace = startLocalTrace("client.get(table {}, consistency {})",
                LoggingArgs.safeTableOrPlaceholder(tableReference), consistency_level)) {
            return KvsProfilingLogger.maybeLog(
                    (KvsProfilingLogger.CallableCheckedException<ColumnOrSuperColumn, TException>)
                        () -> client.get(key, columnPath, consistency_level),
                    (logger, timer) -> logger.log("client.get({}, {}) took {} ms",
                            LoggingArgs.tableRef(tableReference),
                            SafeArg.of("consistency", consistency_level.toString()),
                            LoggingArgs.durationMillis(timer)));
        }
    }

    @Override
    public CASResult cas(TableReference tableReference,
            ByteBuffer key,
            List<Column> expected,
            List<Column> updates,
            ConsistencyLevel serial_consistency_level,
            ConsistencyLevel commit_consistency_level)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException {
        String internalTableName = AbstractKeyValueService.internalTableName(tableReference);

        try (CloseableTrace trace = startLocalTrace("client.cas(table {})",
                LoggingArgs.safeTableOrPlaceholder(tableReference))) {
            return KvsProfilingLogger.maybeLog(
                    (KvsProfilingLogger.CallableCheckedException<CASResult, TException>)
                            () -> client.cas(key, internalTableName, expected, updates, serial_consistency_level,
                                    commit_consistency_level),
                    (logger, timer) -> logger.log("client.cas({}) took {} ms",
                            LoggingArgs.tableRef(tableReference),
                            LoggingArgs.durationMillis(timer)));
        }
    }

    @Override
    public CqlResult execute_cql3_query(CqlQuery cqlQuery,
            Compression compression,
            ConsistencyLevel consistency)
            throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException,
            TException {
        ByteBuffer queryBytes = ByteBuffer.wrap(cqlQuery.fullQuery().getBytes(StandardCharsets.UTF_8));

        try (CloseableTrace trace = startLocalTrace("cqlExecutor.execute_cql3_query(query {})", cqlQuery)) {
            return KvsProfilingLogger.maybeLog(
                    (KvsProfilingLogger.CallableCheckedException<CqlResult, TException>)
                            () -> client.execute_cql3_query(queryBytes, compression, consistency),
                    (logger, timer) -> this.logSlowResult(cqlQuery, logger, timer),
                    this::logResultSize);
        }
    }

    private void logSlowResult(CqlQuery cqlQuery, KvsProfilingLogger.LoggingFunction log, Stopwatch timer) {
        Object[] allArgs = new Object[cqlQuery.queryArgs.length + 3];
        allArgs[0] = SafeArg.of("queryFormat", cqlQuery.queryFormat);
        allArgs[1] = UnsafeArg.of("fullQuery", cqlQuery.fullQuery());
        allArgs[2] = LoggingArgs.durationMillis(timer);
        System.arraycopy(cqlQuery.queryArgs, 0, allArgs, 3, cqlQuery.queryArgs.length);

        log.log("A CQL query was slow: queryFormat = [{}], fullQuery = [{}], durationMillis = {}", allArgs);
    }

    private void logResultSize(KvsProfilingLogger.LoggingFunction log, CqlResult result) {
        log.log("and returned {} rows",
                SafeArg.of("numRows", result.getRows().size()));
    }

    private ColumnParent getColumnParent(TableReference tableRef) {
        return new ColumnParent(AbstractKeyValueService.internalTableName(tableRef));
    }

    private static CloseableTrace startLocalTrace(CharSequence operationFormat, Object... formatArguments) {
        return CloseableTrace.startLocalTrace(SERVICE_NAME, operationFormat, formatArguments);
    }
}
