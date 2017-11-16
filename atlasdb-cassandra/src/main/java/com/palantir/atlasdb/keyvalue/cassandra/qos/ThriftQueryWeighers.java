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
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.Mutation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Suppliers;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClient;
import com.palantir.atlasdb.qos.ImmutableQueryWeight;
import com.palantir.atlasdb.qos.QosClient;
import com.palantir.atlasdb.qos.QueryWeight;

public final class ThriftQueryWeighers {

    private static final Logger log = LoggerFactory.getLogger(CassandraClient.class);

    public static final QueryWeight DEFAULT_ESTIMATED_WEIGHT = ImmutableQueryWeight.builder()
            .numBytes(100)
            .numDistinctRows(1)
            .timeTakenNanos(TimeUnit.MILLISECONDS.toNanos(2))
            .build();

    private ThriftQueryWeighers() { }

    public static final QosClient.QueryWeigher<Map<ByteBuffer, List<ColumnOrSuperColumn>>> MULTIGET_SLICE =
            readWeigher(ThriftObjectSizeUtils::getApproximateReadByteCount, Map::size);

    public static final QosClient.QueryWeigher<List<KeySlice>> GET_RANGE_SLICES =
            readWeigher(ThriftObjectSizeUtils::getApproximateReadByteCount, List::size);

    public static final QosClient.QueryWeigher<ColumnOrSuperColumn> GET =
            readWeigher(ThriftObjectSizeUtils::getColumnOrSuperColumnSize, ignored -> 1);

    public static final QosClient.QueryWeigher<CqlResult> EXECUTE_CQL3_QUERY =
            // TODO(nziebart): we need to inspect the schema to see how many rows there are - a CQL row is NOT a
            // partition. rows here will depend on the type of query executed in CqlExecutor: either (column, ts) pairs,
            // or (key, column, ts) triplets
            readWeigher(ThriftObjectSizeUtils::getCqlResultSize, ignored -> 1);

    public static QosClient.QueryWeigher<Void> batchMutate(
            Map<ByteBuffer, Map<String, List<Mutation>>> mutationMap) {
        long numRows = ThriftObjectSizeUtils.getCollectionSize(mutationMap.values(), map -> (long) map.size());
        return writeWeigher(numRows, () -> ThriftObjectSizeUtils.getApproximateWriteByteCount(mutationMap));
    }

    public static <T> QosClient.QueryWeigher<T> readWeigher(Function<T, Long> bytesRead, Function<T, Integer> numRows) {
        return new QosClient.QueryWeigher<T>() {
            @Override
            public QueryWeight estimate() {
                return DEFAULT_ESTIMATED_WEIGHT;
            }

            @Override
            public QueryWeight weigh(T result, long timeTakenNanos) {
                return ImmutableQueryWeight.builder()
                        .numBytes(safeGetNumBytesOrDefault(() -> bytesRead.apply(result)))
                        .timeTakenNanos((int) timeTakenNanos)
                        .numDistinctRows(numRows.apply(result))
                        .build();
            }
        };
    }

    public static <T> QosClient.QueryWeigher<T> writeWeigher(long numRows, Supplier<Long> bytesWritten) {
        Supplier<Long> weight = Suppliers.memoize(() -> safeGetNumBytesOrDefault(bytesWritten))::get;

        return new QosClient.QueryWeigher<T>() {
            @Override
            public QueryWeight estimate() {
                return ImmutableQueryWeight.builder()
                        .from(DEFAULT_ESTIMATED_WEIGHT)
                        .numBytes(weight.get())
                        .numDistinctRows(numRows)
                        .build();
            }

            @Override
            public QueryWeight weigh(T result, long timeTakenNanos) {
                return ImmutableQueryWeight.builder()
                        .from(estimate())
                        .timeTakenNanos((int) timeTakenNanos)
                        .build();
            }
        };
    }

    // TODO(nziebart): we really shouldn't be needing to catch exceptions here
    private static long safeGetNumBytesOrDefault(Supplier<Long> numBytes) {
        try {
            return numBytes.get();
        } catch (Exception e) {
            log.warn("Error calculating number of bytes", e);
            return DEFAULT_ESTIMATED_WEIGHT.numBytes();
        }
    }

}
