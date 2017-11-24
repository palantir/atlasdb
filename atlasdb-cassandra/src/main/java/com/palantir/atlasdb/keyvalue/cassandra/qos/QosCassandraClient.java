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

package com.palantir.atlasdb.keyvalue.cassandra.qos;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.cassandra.thrift.CASResult;
import org.apache.cassandra.thrift.Cassandra;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClient;
import com.palantir.atlasdb.keyvalue.cassandra.CqlQuery;
import com.palantir.atlasdb.qos.QosClient;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;

@SuppressWarnings({"all"}) // thrift variable names.
public class QosCassandraClient implements CassandraClient {

    private static final Logger log = LoggerFactory.getLogger(CassandraClient.class);
    private static final Function<TableReference, Boolean> ZERO_ESTIMATE_DETERMINING_FUNCTION = tRef ->
            tRef.equals(TransactionConstants.TRANSACTION_TABLE) || tRef.equals(AtlasDbConstants.DEFAULT_METADATA_TABLE);

    private final CassandraClient client;
    private final QosClient qosClient;

    public QosCassandraClient(CassandraClient client, QosClient qosClient) {
        this.client = client;
        this.qosClient = qosClient;
    }

    @Override
    public Cassandra.Client rawClient() {
        return client.rawClient();
    }

    @Override
    public Map<ByteBuffer, List<ColumnOrSuperColumn>> multiget_slice(String kvsMethodName, TableReference tableRef,
            List<ByteBuffer> keys, SlicePredicate predicate, ConsistencyLevel consistency_level)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException {
        return qosClient.executeRead(
                () -> client.multiget_slice(kvsMethodName, tableRef, keys, predicate, consistency_level),
                ThriftQueryWeighers.multigetSlice(keys, ZERO_ESTIMATE_DETERMINING_FUNCTION.apply(tableRef)));
    }

    @Override
    public List<KeySlice> get_range_slices(String kvsMethodName, TableReference tableRef, SlicePredicate predicate,
            KeyRange range, ConsistencyLevel consistency_level)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException {
        return qosClient.executeRead(
                () -> client.get_range_slices(kvsMethodName, tableRef, predicate, range, consistency_level),
                ThriftQueryWeighers.getRangeSlices(range, ZERO_ESTIMATE_DETERMINING_FUNCTION.apply(tableRef)));
    }

    @Override
    public void batch_mutate(String kvsMethodName, Map<ByteBuffer, Map<String, List<Mutation>>> mutation_map,
            ConsistencyLevel consistency_level)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException {
        qosClient.executeWrite(
                () -> {
                    client.batch_mutate(kvsMethodName, mutation_map, consistency_level);
                    return null;
                },
                ThriftQueryWeighers.batchMutate(mutation_map));
    }

    @Override
    public ColumnOrSuperColumn get(TableReference tableReference, ByteBuffer key, byte[] column,
            ConsistencyLevel consistency_level)
            throws InvalidRequestException, NotFoundException, UnavailableException, TimedOutException, TException {
        return qosClient.executeRead(
                () -> client.get(tableReference, key, column, consistency_level),
                ThriftQueryWeighers.get(ZERO_ESTIMATE_DETERMINING_FUNCTION.apply(tableReference)));
    }

    @Override
    public CASResult cas(TableReference tableReference, ByteBuffer key, List<Column> expected, List<Column> updates,
            ConsistencyLevel serial_consistency_level, ConsistencyLevel commit_consistency_level)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException {
        // CAS is intentionally not rate limited, until we have a concept of priority
        return client.cas(tableReference, key, expected, updates, serial_consistency_level, commit_consistency_level);
    }

    @Override
    public CqlResult execute_cql3_query(CqlQuery cqlQuery, Compression compression, ConsistencyLevel consistency)
            throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException,
            TException {
        return qosClient.executeRead(
                () -> client.execute_cql3_query(cqlQuery, compression, consistency),
                ThriftQueryWeighers.EXECUTE_CQL3_QUERY);
    }


}
