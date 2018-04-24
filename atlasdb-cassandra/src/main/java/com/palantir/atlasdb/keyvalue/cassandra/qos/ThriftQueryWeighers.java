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

package com.palantir.atlasdb.keyvalue.cassandra.qos;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.Mutation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClient;
import com.palantir.atlasdb.qos.ImmutableQueryWeight;
import com.palantir.atlasdb.qos.QosClient;
import com.palantir.atlasdb.qos.QueryWeight;

public final class ThriftQueryWeighers {

    private static final Logger log = LoggerFactory.getLogger(CassandraClient.class);

    @VisibleForTesting
    private static final int ESTIMATED_NUM_BYTES_PER_ROW = 100;
    static final QueryWeight DEFAULT_ESTIMATED_WEIGHT = ImmutableQueryWeight.builder()
            .numBytes(ESTIMATED_NUM_BYTES_PER_ROW)
            .numDistinctRows(1)
            .timeTakenNanos(TimeUnit.MILLISECONDS.toNanos(2))
            .build();

    static final QueryWeight ZERO_ESTIMATED_WEIGHT = ImmutableQueryWeight.builder()
            .numBytes(0)
            .numDistinctRows(1)
            .timeTakenNanos(TimeUnit.MILLISECONDS.toNanos(1))
            .build();

    private ThriftQueryWeighers() { }

    static QosClient.QueryWeigher<Map<ByteBuffer, List<ColumnOrSuperColumn>>> multigetSlice(List<ByteBuffer> keys,
            boolean zeroEstimate) {
        return zeroEstimate
                ? readWeigherWithZeroEstimate(ThriftObjectSizeUtils::getApproximateSizeOfColsByKey, Map::size,
                keys.size())
                : readWeigher(ThriftObjectSizeUtils::getApproximateSizeOfColsByKey, Map::size, keys.size());
    }

    static QosClient.QueryWeigher<List<KeySlice>> getRangeSlices(KeyRange keyRange, boolean zeroEstimate) {
        return zeroEstimate
                ? readWeigherWithZeroEstimate(ThriftObjectSizeUtils::getApproximateSizeOfKeySlices, List::size,
                keyRange.count)
                : readWeigher(ThriftObjectSizeUtils::getApproximateSizeOfKeySlices, List::size, keyRange.count);
    }

    static QosClient.QueryWeigher<ColumnOrSuperColumn> get(boolean zeroEstimate) {
        return zeroEstimate
                ? readWeigherWithZeroEstimate(ThriftObjectSizeUtils::getColumnOrSuperColumnSize, ignored -> 1, 1)
                : readWeigher(ThriftObjectSizeUtils::getColumnOrSuperColumnSize, ignored -> 1, 1);
    }

    static final QosClient.QueryWeigher<CqlResult> EXECUTE_CQL3_QUERY =
            // TODO(nziebart): we need to inspect the schema to see how many rows there are - a CQL row is NOT a
            // partition. rows here will depend on the type of query executed in CqlExecutor: either (column, ts) pairs,
            // or (key, column, ts) triplets
            // Currently, transaction or metadata table queries dont use the CQL executor,
            // but we should provide a way to estimate zero based on the tableRef if they do start using it.
            readWeigher(ThriftObjectSizeUtils::getCqlResultSize, ignored -> 1, 1);

    static QosClient.QueryWeigher<Void> batchMutate(Map<ByteBuffer, Map<String, List<Mutation>>> mutationMap) {
        long numRows = mutationMap.size();
        return writeWeigher(numRows, () -> ThriftObjectSizeUtils.getApproximateSizeOfMutationMap(mutationMap));
    }

    private static <T> QosClient.QueryWeigher<T> readWeigherWithZeroEstimate(Function<T, Long> bytesRead,
            Function<T, Integer> numRows,
            int numberOfQueriedRows) {
        return new QosClient.QueryWeigher<T>() {
            @Override
            public QueryWeight estimate() {
                return ZERO_ESTIMATED_WEIGHT;
            }

            @Override
            public QueryWeight weighSuccess(T result, long timeTakenNanos) {
                return ImmutableQueryWeight.builder()
                        .numBytes(safeGetNumBytesOrDefault(() -> bytesRead.apply(result)))
                        .timeTakenNanos(timeTakenNanos)
                        .numDistinctRows(numRows.apply(result))
                        .build();
            }

            @Override
            public QueryWeight weighFailure(Exception error, long timeTakenNanos) {
                return ImmutableQueryWeight.builder()
                        .from(DEFAULT_ESTIMATED_WEIGHT)
                        .numBytes(ESTIMATED_NUM_BYTES_PER_ROW * numberOfQueriedRows)
                        .timeTakenNanos(timeTakenNanos)
                        .build();
            }
        };
    }

    private static <T> QosClient.QueryWeigher<T> readWeigher(Function<T, Long> bytesRead,
            Function<T, Integer> numRows,
            int numberOfQueriedRows) {
        return new QosClient.QueryWeigher<T>() {
            @Override
            public QueryWeight estimate() {
                return ImmutableQueryWeight.builder()
                        .from(DEFAULT_ESTIMATED_WEIGHT)
                        .numBytes(ESTIMATED_NUM_BYTES_PER_ROW * numberOfQueriedRows)
                        .build();
            }

            @Override
            public QueryWeight weighSuccess(T result, long timeTakenNanos) {
                return ImmutableQueryWeight.builder()
                        .numBytes(safeGetNumBytesOrDefault(() -> bytesRead.apply(result)))
                        .timeTakenNanos(timeTakenNanos)
                        .numDistinctRows(numRows.apply(result))
                        .build();
            }

            @Override
            public QueryWeight weighFailure(Exception error, long timeTakenNanos) {
                return ImmutableQueryWeight.builder()
                        .from(estimate())
                        .timeTakenNanos(timeTakenNanos)
                        .build();
            }
        };
    }

    private static <T> QosClient.QueryWeigher<T> writeWeigher(long numRows, Supplier<Long> bytesWritten) {
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
            public QueryWeight weighSuccess(T result, long timeTakenNanos) {
                return ImmutableQueryWeight.builder()
                        .from(estimate())
                        .timeTakenNanos(timeTakenNanos)
                        .build();
            }

            @Override
            public QueryWeight weighFailure(Exception error, long timeTakenNanos) {
                return ImmutableQueryWeight.builder()
                        .from(estimate())
                        .timeTakenNanos(timeTakenNanos)
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
