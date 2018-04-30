/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
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
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlPreparedResult;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.InvalidRequestException;
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

import com.palantir.atlasdb.keyvalue.api.TableReference;

@SuppressWarnings({"all"}) // thrift variable names.
public interface CassandraClient {
    /**
     * Checks if the client has a valid connection to Cassandra cluster. Can be used by a client pool
     * to eliminate clients in bad state.
     *
     * @return true if client is in good state.
     */
    boolean isValid();

    Map<ByteBuffer, List<ColumnOrSuperColumn>> multiget_slice(String kvsMethodName,
            TableReference tableRef,
            List<ByteBuffer> keys,
            SlicePredicate predicate, ConsistencyLevel consistency_level)
            throws InvalidRequestException, UnavailableException, TimedOutException, org.apache.thrift.TException;

    List<KeySlice> get_range_slices(String kvsMethodName,
            TableReference tableRef,
            SlicePredicate predicate,
            KeyRange range,
            ConsistencyLevel consistency_level)
            throws InvalidRequestException, UnavailableException, TimedOutException, org.apache.thrift.TException;

    void batch_mutate(String kvsMethodName,
            Map<ByteBuffer, Map<String, List<Mutation>>> mutation_map,
            ConsistencyLevel consistency_level)
            throws InvalidRequestException, UnavailableException, TimedOutException, org.apache.thrift.TException;

    ColumnOrSuperColumn get(TableReference tableReference,
            ByteBuffer key,
            byte[] column,
            ConsistencyLevel consistency_level)
            throws InvalidRequestException, NotFoundException, UnavailableException, TimedOutException,
            org.apache.thrift.TException;

    CASResult cas(TableReference tableReference,
            ByteBuffer key,
            List<Column> expected,
            List<Column> updates,
            ConsistencyLevel serial_consistency_level,
            ConsistencyLevel commit_consistency_level)
            throws InvalidRequestException, UnavailableException, TimedOutException, org.apache.thrift.TException;

    CqlResult execute_cql3_query(CqlQuery cqlQuery,
            Compression compression,
            ConsistencyLevel consistency)
            throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException,
            org.apache.thrift.TException;

    TProtocol getOutputProtocol();

    TProtocol getInputProtocol();

    List<TokenRange> describe_ring(String keyspace) throws InvalidRequestException, TException;

    String describe_version() throws TException;

    Map<String, List<String>> describe_schema_versions() throws InvalidRequestException, TException;

    String describe_partitioner() throws TException;

    KsDef describe_keyspace(String keyspace)
            throws NotFoundException, InvalidRequestException, TException;

    List<KsDef> describe_keyspaces() throws InvalidRequestException, TException;

    String system_add_keyspace(KsDef ks_def)
            throws InvalidRequestException, SchemaDisagreementException, TException;

    String system_update_keyspace(KsDef ks_def)
            throws InvalidRequestException, SchemaDisagreementException, TException;

    String system_add_column_family(CfDef cf_def)
            throws InvalidRequestException, SchemaDisagreementException, TException;

    String system_update_column_family(CfDef cf_def)
            throws InvalidRequestException, SchemaDisagreementException, TException;

    String system_drop_column_family(String column_family)
            throws InvalidRequestException, SchemaDisagreementException, TException;

    CqlPreparedResult prepare_cql3_query(ByteBuffer query, Compression compression)
            throws InvalidRequestException, TException;

    CqlResult execute_prepared_cql3_query(int intemId, List<ByteBuffer> values, ConsistencyLevel consistency)
            throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException;

    ByteBuffer trace_next_query() throws TException;

    void truncate(String cfname)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException;

}
