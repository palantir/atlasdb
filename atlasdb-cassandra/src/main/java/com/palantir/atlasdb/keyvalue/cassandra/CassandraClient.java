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

import org.apache.cassandra.thrift.AutoDelegate_Client;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.commons.lang.SerializationUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.qos.AtlasDbQosClient;
import com.palantir.processors.AutoDelegate;

/**
 * Wrapper for Cassandra.Client.
 */
@AutoDelegate(typeToExtend = Cassandra.Client.class)
@SuppressWarnings({"checkstyle:all", "DuplicateThrows"}) // :'(
public class CassandraClient extends AutoDelegate_Client {
    private final Logger log = LoggerFactory.getLogger(CassandraClient.class);

    private final Cassandra.Client delegate;
    private final AtlasDbQosClient qosClient;
    private final QosMetrics qosMetrics;

    public CassandraClient(Cassandra.Client delegate, AtlasDbQosClient qosClient) {
        super(delegate.getInputProtocol());
        this.delegate = delegate;
        this.qosClient = qosClient;
        this.qosMetrics = new QosMetrics();
    }

    @Override
    public Cassandra.Client delegate() {
        return delegate;
    }

    @Override
    public Map<ByteBuffer, List<ColumnOrSuperColumn>> multiget_slice(List<ByteBuffer> keys, ColumnParent column_parent,
            SlicePredicate predicate, ConsistencyLevel consistency_level)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException {
        qosClient.checkLimit();
        Map<ByteBuffer, List<ColumnOrSuperColumn>> result = delegate.multiget_slice(keys, column_parent,
                predicate, consistency_level);
        try {
            recordBytesRead(getApproximateReadByteCount(result));
        } catch (Exception e) {
            log.warn("Encountered an exception when recording write metrics for multiget_slice.", e);
        }
        return result;
    }

    private long getApproximateReadByteCount(Map<ByteBuffer, List<ColumnOrSuperColumn>> result) {
        return result.entrySet().stream()
                .mapToLong((entry) -> getRowKeySize(entry) + totalSizeOfValues(entry))
                .sum();
    }

    private long getRowKeySize(Map.Entry<ByteBuffer, List<ColumnOrSuperColumn>> entry) {
        return ThriftObjectSizeUtils.getByteBufferSize(entry.getKey());
    }

    private long totalSizeOfValues(Map.Entry<ByteBuffer, List<ColumnOrSuperColumn>> entry) {
        return entry.getValue().stream()
                .mapToLong(ThriftObjectSizeUtils::getSizeOfColumnOrSuperColumn)
                .sum();
    }

    @Override
    public void batch_mutate(Map<ByteBuffer, Map<String, List<Mutation>>> mutation_map,
            ConsistencyLevel consistency_level)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException {
        qosClient.checkLimit();
        delegate.batch_mutate(mutation_map, consistency_level);
        try {
            recordBytesWritten(getApproximateWriteByteCount(mutation_map));
        } catch (Exception e) {
            log.warn("Encountered an exception when recording write metrics for batch_mutate.", e);
        }
    }

    private int getApproximateWriteByteCount(Map<ByteBuffer, Map<String, List<Mutation>>> mutation_map) {
        int approxBytesForKeys = mutation_map.keySet().stream().mapToInt(e -> e.array().length).sum();
        int approxBytesForValues = mutation_map.values().stream()
                .mapToInt(singleMap -> {
                    int approximateBytesInStrings = singleMap.keySet().stream().mapToInt(String::length).sum();
                    int approximateBytesInMutations = singleMap.values().stream()
                            .mapToInt(listOfMutations -> listOfMutations.stream()
                                    .mapToInt(mutation -> SerializationUtils.serialize(mutation).length)
                                    .sum())
                            .sum();
                    return approximateBytesInStrings + approximateBytesInMutations;
                })
                .sum();
        return approxBytesForKeys + approxBytesForValues;
    }

    @Override
    public CqlResult execute_cql3_query(ByteBuffer query, Compression compression, ConsistencyLevel consistency)
            throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException,
            TException {
        qosClient.checkLimit();
        CqlResult cqlResult = delegate.execute_cql3_query(query, compression, consistency);
        try {
            recordBytesRead(SerializationUtils.serialize(cqlResult).length);
        } catch (Exception e) {
            log.warn("Encountered an exception when recording read metrics for execute_cql3_query.", e);
        }
        return cqlResult;
    }

    @Override
    public List<KeySlice> get_range_slices(ColumnParent column_parent, SlicePredicate predicate, KeyRange range,
            ConsistencyLevel consistency_level)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException {
        qosClient.checkLimit();
        List<KeySlice> result = super.get_range_slices(column_parent, predicate, range, consistency_level);
        int approximateBytesRead = result.stream()
                .mapToInt(keySlice -> SerializationUtils.serialize(keySlice).length)
                .sum();
        try {
            recordBytesRead(approximateBytesRead);
        } catch (Exception e) {
            log.warn("Encountered an exception when recording read metrics for get_range_slices.", e);
        }
        return result;
    }

    private void recordBytesRead(long numBytesRead) {
        qosMetrics.updateReadCount();
        qosMetrics.updateBytesRead(numBytesRead);
    }

    private void recordBytesWritten(long numBytesWitten) {
        qosMetrics.updateWriteCount();
        qosMetrics.updateBytesWritten(numBytesWitten);
    }
}
