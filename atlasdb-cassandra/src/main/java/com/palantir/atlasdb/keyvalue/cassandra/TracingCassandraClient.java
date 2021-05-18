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

import com.google.errorprone.annotations.CompileTimeConstant;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.tracing.Tracing.FunctionalTagTranslator;
import com.palantir.atlasdb.tracing.Tracing.TagConsumer;
import com.palantir.tracing.CloseableTracer;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.cassandra.thrift.CASResult;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeyPredicate;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;

@SuppressWarnings({"all"}) // thrift variable names.
public class TracingCassandraClient implements AutoDelegate_CassandraClient {
    private static final String SERVICE_NAME = "cassandra-thrift-client";

    private final CassandraClient client;

    public TracingCassandraClient(CassandraClient client) {
        this.client = client;
    }

    @Override
    public CassandraClient delegate() {
        return this.client;
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

        try (CloseableTracer trace = startLocalTrace("client.multiget_slice", sink -> {
            sink.tableRef(tableRef);
            sink.integer("keys", numberOfKeys);
            sink.integer("columns", numberOfColumns);
            sink.accept("consistency", consistency_level.name());
            sink.accept("kvs", kvsMethodName);
        })) {
            return client.multiget_slice(kvsMethodName, tableRef, keys, predicate, consistency_level);
        }
    }

    @Override
    public Map<ByteBuffer, List<List<ColumnOrSuperColumn>>> multiget_multislice(
            String kvsMethodName,
            TableReference tableRef,
            List<KeyPredicate> keyPredicates,
            ConsistencyLevel consistency_level)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException {
        int numberOfKeyPredicates = keyPredicates.size();

        try (CloseableTracer trace = startLocalTrace("client.multiget_slice", sink -> {
            sink.tableRef(tableRef);
            sink.size("key_predicates", keyPredicates);
            sink.accept("consistency", consistency_level.name());
            sink.accept("kvs", kvsMethodName);
        })) {
            return client.multiget_multislice(kvsMethodName, tableRef, keyPredicates, consistency_level);
        }
    }

    @Override
    public List<KeySlice> get_range_slices(
            String kvsMethodName,
            TableReference tableRef,
            SlicePredicate predicate,
            KeyRange range,
            ConsistencyLevel consistency_level)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException {
        int numberOfColumns = predicate.slice_range.count;
        int batchHint = range.count;

        try (CloseableTracer trace = startLocalTrace("client.get_range_slices", sink -> {
            sink.tableRef(tableRef);
            sink.integer("columns", numberOfColumns);
            sink.integer("batchHint", batchHint);
            sink.accept("consistency", consistency_level.name());
            sink.accept("kvs", kvsMethodName);
        })) {
            return client.get_range_slices(kvsMethodName, tableRef, predicate, range, consistency_level);
        }
    }

    @Override
    public void remove(
            String kvsMethodName,
            TableReference tableRef,
            byte[] row,
            long timestamp,
            ConsistencyLevel consistency_level)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException {
        try (CloseableTracer trace = startLocalTrace("client.remove", sink -> {
            sink.accept("consistency", consistency_level.name());
            sink.accept("kvs", kvsMethodName);
        })) {
            client.remove(kvsMethodName, tableRef, row, timestamp, consistency_level);
        }
    }

    @Override
    public void batch_mutate(
            String kvsMethodName,
            Map<ByteBuffer, Map<String, List<Mutation>>> mutation_map,
            ConsistencyLevel consistency_level)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException {
        int numberOfRowsMutated = mutation_map.size();

        try (CloseableTracer trace = startLocalTrace("client.batch_mutate", sink -> {
            sink.integer("numberOfRowsMutated", numberOfRowsMutated);
            sink.accept("consistency", consistency_level.name());
            sink.accept("kvs", kvsMethodName);
        })) {
            client.batch_mutate(kvsMethodName, mutation_map, consistency_level);
        }
    }

    @Override
    public ColumnOrSuperColumn get(
            TableReference tableReference, ByteBuffer key, byte[] column, ConsistencyLevel consistency_level)
            throws InvalidRequestException, NotFoundException, UnavailableException, TimedOutException, TException {
        try (CloseableTracer trace = startLocalTrace("client.get", sink -> {
            sink.tableRef(tableReference);
            sink.accept("consistency", consistency_level.name());
        })) {
            return client.get(tableReference, key, column, consistency_level);
        }
    }

    @Override
    public CASResult cas(
            TableReference tableReference,
            ByteBuffer key,
            List<Column> expected,
            List<Column> updates,
            ConsistencyLevel serial_consistency_level,
            ConsistencyLevel commit_consistency_level)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException {
        try (CloseableTracer trace = startLocalTrace("client.cas", sink -> {
            sink.tableRef(tableReference);
        })) {
            return client.cas(
                    tableReference, key, expected, updates, serial_consistency_level, commit_consistency_level);
        }
    }

    @Override
    public CqlResult execute_cql3_query(CqlQuery cqlQuery, Compression compression, ConsistencyLevel consistency)
            throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException,
                    TException {
        try (CloseableTracer trace = startLocalTrace("cqlExecutor.execute_cql3_query", sink -> {
            sink.accept("query", cqlQuery.getSafeLog());
        })) {
            return client.execute_cql3_query(cqlQuery, compression, consistency);
        }
    }

    private static CloseableTracer startLocalTrace(
            @CompileTimeConstant String operation, Consumer<TagConsumer> tagTranslator) {
        return CloseableTracer.startSpan(getOperation(operation), FunctionalTagTranslator.INSTANCE, tagTranslator);
    }

    private static String getOperation(@CompileTimeConstant String operation) {
        return SERVICE_NAME + "." + operation;
    }
}
