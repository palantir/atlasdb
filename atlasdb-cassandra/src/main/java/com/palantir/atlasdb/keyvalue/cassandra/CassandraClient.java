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
        recordBytesRead(getApproximateReadByteCount(result));
        return result;
    }

    private long getApproximateReadByteCount(Map<ByteBuffer, List<ColumnOrSuperColumn>> result) {
        return getCollectionSize(result.entrySet(),
                rowResult ->
                    ThriftObjectSizeUtils.getByteBufferSize(rowResult.getKey())
                    + getCollectionSize(rowResult.getValue(), ThriftObjectSizeUtils::getColumnOrSuperColumnSize));
    }

    @Override
    public void batch_mutate(Map<ByteBuffer, Map<String, List<Mutation>>> mutationMap,
            ConsistencyLevel consistency_level)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException {
        qosClient.checkLimit();
        delegate.batch_mutate(mutationMap, consistency_level);
        recordBytesWritten(getApproximateWriteByteCount(mutationMap));
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
    public CqlResult execute_cql3_query(ByteBuffer query, Compression compression, ConsistencyLevel consistency)
            throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException,
            TException {
        qosClient.checkLimit();
        CqlResult cqlResult = delegate.execute_cql3_query(query, compression, consistency);
        recordBytesRead(ThriftObjectSizeUtils.getCqlResultSize(cqlResult));
        return cqlResult;
    }

    @Override
    public List<KeySlice> get_range_slices(ColumnParent column_parent, SlicePredicate predicate, KeyRange range,
            ConsistencyLevel consistency_level)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException {
        qosClient.checkLimit();
        List<KeySlice> result = super.get_range_slices(column_parent, predicate, range, consistency_level);
        recordBytesRead(getCollectionSize(result, ThriftObjectSizeUtils::getKeySliceSize));
        return result;
    }

    private void recordBytesRead(long numBytesRead) {
        try {
            qosMetrics.updateReadCount();
            qosMetrics.updateBytesRead(numBytesRead);
        } catch (Exception e) {
            log.warn("Encountered an exception when recording read metrics.", e);
        }
    }

    private void recordBytesWritten(long numBytesWritten) {
        try {
            qosMetrics.updateWriteCount();
            qosMetrics.updateBytesWritten(numBytesWritten);
        } catch (Exception e) {
            log.warn("Encountered an exception when recording write metrics.", e);
        }
    }

    private <T> long getCollectionSize(Collection<T> collection, Function<T, Long> singleObjectSizeFunction) {
        return collection.stream().mapToLong(singleObjectSizeFunction::apply).sum();
    }
}
