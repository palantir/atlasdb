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

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.AbstractKeyValueService;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.cassandra.thrift.CASResult;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlPreparedResult;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeyPredicate;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

@SuppressWarnings({"all"}) // thrift variable names.
public class CassandraClientImpl implements CassandraClient {
    private final Cassandra.Client client;
    private volatile AtomicReference<Throwable> invalidated = new AtomicReference<>();

    // Client is considered to be invalid if a blacklisted exception is thrown.
    private static final ImmutableSet<Class> BLACKLISTED_EXCEPTIONS =
            ImmutableSet.of(TTransportException.class, TProtocolException.class, NoSuchElementException.class);

    public CassandraClientImpl(Cassandra.Client client) {
        this.client = client;
    }

    @Override
    public boolean isValid() {
        return this.invalidated.get() == null;
    }

    @Override
    public Map<ByteBuffer, List<ColumnOrSuperColumn>> multiget_slice(
            String kvsMethodName,
            TableReference tableRef,
            List<ByteBuffer> keys,
            SlicePredicate predicate,
            ConsistencyLevel consistency_level)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException {
        ColumnParent colFam = getColumnParent(tableRef);

        return executeHandlingExceptions(() -> client.multiget_slice(keys, colFam, predicate, consistency_level));
    }

    @Override
    public Map<ByteBuffer, List<List<ColumnOrSuperColumn>>> multiget_multislice(
            String kvsMethodName,
            TableReference tableRef,
            List<KeyPredicate> keyPredicates,
            ConsistencyLevel consistency_level)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException {
        ColumnParent colFam = getColumnParent(tableRef);

        return executeHandlingExceptions(() -> client.multiget_multislice(keyPredicates, colFam, consistency_level));
    }

    @Override
    public List<KeySlice> get_range_slices(
            String kvsMethodName,
            TableReference tableRef,
            SlicePredicate predicate,
            KeyRange range,
            ConsistencyLevel consistency_level)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException {
        ColumnParent colFam = getColumnParent(tableRef);

        return executeHandlingExceptions(() -> client.get_range_slices(colFam, predicate, range, consistency_level));
    }

    @Override
    public String system_add_keyspace(KsDef ks_def)
            throws InvalidRequestException, SchemaDisagreementException, TException {
        return executeHandlingExceptions(() -> client.system_add_keyspace(ks_def));
    }

    @Override
    public List<KsDef> describe_keyspaces() throws InvalidRequestException, TException {
        return executeHandlingExceptions(client::describe_keyspaces);
    }

    @Override
    public void batch_mutate(
            String kvsMethodName,
            Map<ByteBuffer, Map<String, List<Mutation>>> mutation_map,
            ConsistencyLevel consistency_level)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException {
        executeHandlingExceptions(() -> client.batch_mutate(mutation_map, consistency_level));
    }

    @Override
    public ColumnOrSuperColumn get(
            TableReference tableReference, ByteBuffer key, byte[] column, ConsistencyLevel consistency_level)
            throws InvalidRequestException, NotFoundException, UnavailableException, TimedOutException, TException {
        ColumnPath columnPath = new ColumnPath(tableReference.getQualifiedName());
        columnPath.setColumn(column);

        return executeHandlingExceptions(() -> client.get(key, columnPath, consistency_level));
    }

    @Override
    public void remove(
            String kvsMethodName,
            TableReference tableRef,
            byte[] row,
            long timestamp,
            ConsistencyLevel consistency_level)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException {
        String internalTableName = AbstractKeyValueService.internalTableName(tableRef);
        ColumnPath columnPath = new ColumnPath();
        columnPath.setColumn_family(internalTableName);
        executeHandlingExceptions(() -> client.remove(ByteBuffer.wrap(row), columnPath, timestamp, consistency_level));
    }

    @Override
    public TProtocol getOutputProtocol() {
        return executeMethodWithoutException(client::getOutputProtocol);
    }

    @Override
    public TProtocol getInputProtocol() {
        return executeMethodWithoutException(client::getInputProtocol);
    }

    @Override
    public KsDef describe_keyspace(String keyspace) throws NotFoundException, InvalidRequestException, TException {
        return executeHandlingExceptions(() -> client.describe_keyspace(keyspace));
    }

    @Override
    public String system_drop_column_family(String column_family)
            throws InvalidRequestException, SchemaDisagreementException, TException {
        return executeHandlingExceptions(() -> client.system_drop_column_family(column_family));
    }

    @Override
    public void truncate(String cfname)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException {
        executeHandlingExceptions(() -> client.truncate(cfname));
    }

    @Override
    public String system_update_column_family(CfDef cf_def)
            throws InvalidRequestException, SchemaDisagreementException, TException {
        return executeHandlingExceptions(() -> client.system_update_column_family(cf_def));
    }

    @Override
    public String describe_partitioner() throws TException {
        return executeHandlingExceptions(client::describe_partitioner);
    }

    @Override
    public List<TokenRange> describe_ring(String keyspace) throws InvalidRequestException, TException {
        return executeHandlingExceptions(() -> client.describe_ring(keyspace));
    }

    @Override
    public String describe_snitch() throws TException {
        return client.describe_snitch();
    }

    @Override
    public String describe_version() throws TException {
        return executeHandlingExceptions(client::describe_version);
    }

    @Override
    public String system_update_keyspace(KsDef ks_def)
            throws InvalidRequestException, SchemaDisagreementException, TException {
        return executeHandlingExceptions(() -> client.system_update_keyspace(ks_def));
    }

    @Override
    public CqlPreparedResult prepare_cql3_query(ByteBuffer query, Compression compression)
            throws InvalidRequestException, TException {
        return executeHandlingExceptions(() -> client.prepare_cql3_query(query, compression));
    }

    @Override
    public CqlResult execute_prepared_cql3_query(int intemId, List<ByteBuffer> values, ConsistencyLevel consistency)
            throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException,
                    TException {
        return executeHandlingExceptions(() -> client.execute_prepared_cql3_query(intemId, values, consistency));
    }

    @Override
    public ByteBuffer trace_next_query() throws TException {
        return executeHandlingExceptions(client::trace_next_query);
    }

    @Override
    public Map<String, List<String>> describe_schema_versions() throws InvalidRequestException, TException {
        return executeHandlingExceptions(client::describe_schema_versions);
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
        String internalTableName = AbstractKeyValueService.internalTableName(tableReference);

        return executeHandlingExceptions(() -> client.cas(
                key, internalTableName, expected, updates, serial_consistency_level, commit_consistency_level));
    }

    @Override
    public CASResult put_unless_exists(
            TableReference tableReference,
            ByteBuffer key,
            List<Column> updates,
            ConsistencyLevel serial_consistency_level,
            ConsistencyLevel commit_consistency_level)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException {
        String internalTableName = AbstractKeyValueService.internalTableName(tableReference);

        return executeHandlingExceptions(() -> client.put_unless_exists(
                key, internalTableName, updates, serial_consistency_level, commit_consistency_level));
    }

    @Override
    public CqlResult execute_cql3_query(CqlQuery cqlQuery, Compression compression, ConsistencyLevel consistency)
            throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException,
                    TException {
        ByteBuffer queryBytes = ByteBuffer.wrap(cqlQuery.toString().getBytes(StandardCharsets.UTF_8));

        return executeHandlingExceptions(() -> client.execute_cql3_query(queryBytes, compression, consistency));
    }

    @Override
    public void close() {
        TProtocol inputProtocol = getInputProtocol();
        TProtocol outputProtocol = getOutputProtocol();
        try (TTransport inputTransport = inputProtocol.getTransport();
                TTransport outputTransport = outputProtocol.getTransport()) {
            inputProtocol.reset();
            outputProtocol.reset();
        }
    }

    private ColumnParent getColumnParent(TableReference tableRef) {
        return new ColumnParent(AbstractKeyValueService.internalTableName(tableRef));
    }

    private <T> T executeHandlingExceptions(ThrowingSupplier<T> supplier)
            throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException,
                    TException {
        checkIfValidClient();
        try {
            return supplier.apply();
        } catch (Exception e) {
            updateIsValid(e);
            throw e;
        }
    }

    private void executeHandlingExceptions(ThrowingVoidSupplier supplier)
            throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException,
                    TException {
        checkIfValidClient();
        try {
            supplier.apply();
        } catch (Exception e) {
            updateIsValid(e);
            throw e;
        }
    }

    private <T> T executeMethodWithoutException(Supplier<T> supplier) {
        checkIfValidClient();
        try {
            return supplier.get();
        } catch (Exception e) {
            updateIsValid(e);
            throw e;
        }
    }

    private interface ThrowingVoidSupplier {
        void apply()
                throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException,
                        TException;
    }

    private interface ThrowingSupplier<T> {
        T apply()
                throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException,
                        TException;
    }

    private void updateIsValid(Exception e) {
        if (invalidated.get() != null) {
            return;
        }
        for (Class b : BLACKLISTED_EXCEPTIONS) {
            if (b.isInstance(e)) {
                invalidated.compareAndSet(null, new Throwable("Client invalidated here.", e));
                return;
            }
        }
    }

    private void checkIfValidClient() {
        Throwable localInvalidated = invalidated.get();
        if (localInvalidated != null) {
            throw new SafeIllegalStateException("Method execution on invalid client", localInvalidated);
        }
    }
}
