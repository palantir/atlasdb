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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

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

import com.palantir.atlasdb.keyvalue.api.TableReference;

@SuppressWarnings({"all"}) // thrift variable names.
public class QosCassandraClient implements CassandraClient {
    private final Logger log = LoggerFactory.getLogger(CassandraClient.class);

    private final CassandraClient client;
    private final QosMetrics qosMetrics;

    public QosCassandraClient(CassandraClient client) {
        this.client = client;
        qosMetrics = new QosMetrics();
    }

    @Override
    public Cassandra.Client rawClient() {
        return client.rawClient();
    }

    @Override
    public Map<ByteBuffer, List<ColumnOrSuperColumn>> multiget_slice(String kvsMethodName, TableReference tableRef,
            List<ByteBuffer> keys, SlicePredicate predicate, ConsistencyLevel consistency_level)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException {
        Map<ByteBuffer, List<ColumnOrSuperColumn>> result = client.multiget_slice(kvsMethodName, tableRef, keys,
                predicate, consistency_level);
        recordBytesRead(() -> getApproximateReadByteCount(result));
        return result;
    }

    private long getApproximateReadByteCount(Map<ByteBuffer, List<ColumnOrSuperColumn>> result) {
        return getCollectionSize(result.entrySet(),
                rowResult -> ThriftObjectSizeUtils.getByteBufferSize(rowResult.getKey())
                        + getCollectionSize(rowResult.getValue(),
                        ThriftObjectSizeUtils::getColumnOrSuperColumnSize));
    }

    @Override
    public List<KeySlice> get_range_slices(String kvsMethodName, TableReference tableRef, SlicePredicate predicate,
            KeyRange range, ConsistencyLevel consistency_level)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException {
        List<KeySlice> result = client.get_range_slices(kvsMethodName, tableRef, predicate, range, consistency_level);
        recordBytesRead(() -> getCollectionSize(result, ThriftObjectSizeUtils::getKeySliceSize));
        return result;
    }

    @Override
    public void batch_mutate(String kvsMethodName, Map<ByteBuffer, Map<String, List<Mutation>>> mutation_map,
            ConsistencyLevel consistency_level)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException {
        client.batch_mutate(kvsMethodName, mutation_map, consistency_level);
        recordBytesWritten(() -> getApproximateWriteByteCount(mutation_map));
    }

    private long getApproximateWriteByteCount(Map<ByteBuffer, Map<String, List<Mutation>>> batchMutateMap) {
        long approxBytesForKeys = getCollectionSize(batchMutateMap.keySet(), ThriftObjectSizeUtils::getByteBufferSize);
        long approxBytesForValues = getCollectionSize(batchMutateMap.values(), currentMap ->
                getCollectionSize(currentMap.keySet(), ThriftObjectSizeUtils::getStringSize)
                        + getCollectionSize(currentMap.values(),
                        mutations -> getCollectionSize(mutations, ThriftObjectSizeUtils::getMutationSize)));
        return approxBytesForKeys + approxBytesForValues;
    }

    @Override
    public ColumnOrSuperColumn get(TableReference tableReference, ByteBuffer key, byte[] column,
            ConsistencyLevel consistency_level)
            throws InvalidRequestException, NotFoundException, UnavailableException, TimedOutException, TException {
        ColumnOrSuperColumn result = client.get(tableReference, key, column, consistency_level);
        recordBytesRead(() -> ThriftObjectSizeUtils.getColumnOrSuperColumnSize(result));
        return result;
    }

    @Override
    public CASResult cas(TableReference tableReference, ByteBuffer key, List<Column> expected, List<Column> updates,
            ConsistencyLevel serial_consistency_level, ConsistencyLevel commit_consistency_level)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException {
        CASResult result = client.cas(tableReference, key, expected, updates, serial_consistency_level,
                commit_consistency_level);
        recordBytesWritten(() -> getCollectionSize(updates, ThriftObjectSizeUtils::getColumnSize));
        recordBytesRead(() -> getCollectionSize(updates, ThriftObjectSizeUtils::getColumnSize));
        return result;
    }

    @Override
    public CqlResult execute_cql3_query(CqlQuery cqlQuery, Compression compression, ConsistencyLevel consistency)
            throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException,
            TException {
        CqlResult cqlResult = client.execute_cql3_query(cqlQuery, compression, consistency);
        recordBytesRead(() -> ThriftObjectSizeUtils.getCqlResultSize(cqlResult));
        return cqlResult;
    }

    private void recordBytesRead(Supplier<Long> numBytesRead) {
        try {
            qosMetrics.updateReadCount();
            qosMetrics.updateBytesRead(numBytesRead.get());
        } catch (Exception e) {
            log.warn("Encountered an exception when recording read metrics.", e);
        }
    }

    private void recordBytesWritten(Supplier<Long> numBytesWritten) {
        try {
            qosMetrics.updateWriteCount();
            qosMetrics.updateBytesWritten(numBytesWritten.get());
        } catch (Exception e) {
            log.warn("Encountered an exception when recording write metrics.", e);
        }
    }

    private <T> long getCollectionSize(Collection<T> collection, Function<T, Long> singleObjectSizeFunction) {
        return ThriftObjectSizeUtils.getCollectionSize(collection, singleObjectSizeFunction);
    }
}
