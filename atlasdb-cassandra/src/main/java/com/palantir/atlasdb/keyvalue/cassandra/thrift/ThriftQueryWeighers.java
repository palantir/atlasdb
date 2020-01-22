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
package com.palantir.atlasdb.keyvalue.cassandra.thrift;

import com.google.common.base.Suppliers;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClient;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.Mutation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ThriftQueryWeighers {
    private static final Logger log = LoggerFactory.getLogger(CassandraClient.class);

    static final int ESTIMATED_NUM_BYTES_PER_ROW = 100;

    private ThriftQueryWeighers() { }

    public static QueryWeigher<Map<ByteBuffer, List<ColumnOrSuperColumn>>> multigetSlice(List<ByteBuffer> keys) {
        return readWeigher(ThriftObjectSizeUtils::getApproximateSizeOfColsByKey, Map::size, keys.size());
    }

    public static QueryWeigher<List<KeySlice>> getRangeSlices(KeyRange keyRange) {
        return readWeigher(ThriftObjectSizeUtils::getApproximateSizeOfKeySlices, List::size, keyRange.count);
    }

    public static final QueryWeigher<ColumnOrSuperColumn> GET =
            readWeigher(ThriftObjectSizeUtils::getColumnOrSuperColumnSize, ignored -> 1, 1);

    public static final QueryWeigher<CqlResult> EXECUTE_CQL3_QUERY =
            // TODO(nziebart): we need to inspect the schema to see how many rows there are - a CQL row is NOT a
            // partition. rows here will depend on the type of query executed in CqlExecutor: either (column, ts) pairs,
            // or (key, column, ts) triplets
            // Currently, transaction or metadata table queries dont use the CQL executor,
            // but we should provide a way to estimate zero based on the tableRef if they do start using it.
            readWeigher(ThriftObjectSizeUtils::getCqlResultSize, ignored -> 1, 1);

    public static QueryWeigher<Void> batchMutate(Map<ByteBuffer, Map<String, List<Mutation>>> mutationMap) {
        long numRows = mutationMap.size();
        return writeWeigher(numRows, () -> ThriftObjectSizeUtils.getApproximateSizeOfMutationMap(mutationMap));
    }

    public static QueryWeigher<Void> remove(byte[] row) {
        return writeWeigher(1, () -> (long) row.length);
    }

    private static <T> QueryWeigher<T> readWeigher(Function<T, Long> bytesRead,
            Function<T, Integer> numRows,
            int numberOfQueriedRows) {
        return new QueryWeigher<T>() {

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
                        .numBytes(ESTIMATED_NUM_BYTES_PER_ROW * numberOfQueriedRows)
                        .timeTakenNanos(timeTakenNanos)
                        .numDistinctRows(1)
                        .build();
            }
        };
    }

    private static <T> QueryWeigher<T> writeWeigher(long numRows, Supplier<Long> bytesWritten) {
        Supplier<Long> weight = Suppliers.memoize(() -> safeGetNumBytesOrDefault(bytesWritten));

        return new QueryWeigher<T>() {

            @Override
            public QueryWeight weighSuccess(T result, long timeTakenNanos) {
                return ImmutableQueryWeight.builder()
                        .numBytes(weight.get())
                        .numDistinctRows(numRows)
                        .timeTakenNanos(timeTakenNanos)
                        .build();
            }

            @Override
            public QueryWeight weighFailure(Exception error, long timeTakenNanos) {
                return weighSuccess(null, timeTakenNanos);
            }
        };
    }

    // TODO(nziebart): we really shouldn't be needing to catch exceptions here
    private static long safeGetNumBytesOrDefault(Supplier<Long> numBytes) {
        try {
            return numBytes.get();
        } catch (Exception e) {
            log.warn("Error calculating number of bytes", e);
            return ESTIMATED_NUM_BYTES_PER_ROW;
        }
    }

    public interface QueryWeigher<T> {
        QueryWeight weighSuccess(T result, long timeTakenNanos);
        QueryWeight weighFailure(Exception error, long timeTakenNanos);
    }
}
