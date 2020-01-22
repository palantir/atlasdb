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

import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueServices;
import java.nio.ByteBuffer;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.immutables.value.Value;

public final class SlicePredicates {

    private SlicePredicates() { }

    public static SlicePredicate latestVersionForColumn(byte[] columnName, long maxTimestampExclusive) {
        return create(
                Range.singleColumn(columnName, maxTimestampExclusive),
                Limit.ONE);
    }

    public static SlicePredicate rangeTombstoneForColumn(byte[] columnName, long maxTimestampExclusive) {
        return create(
                Range.of(
                        Range.startOfColumn(columnName, maxTimestampExclusive),
                        Range.endOfColumnExcludingSentinels(columnName)),
                Limit.NO_LIMIT);
    }

    public static SlicePredicate rangeTombstoneIncludingSentinelForColumn(byte[] columnName,
            long maxTimestampExclusive) {
        return create(
                Range.of(
                        Range.startOfColumn(columnName, maxTimestampExclusive),
                        Range.endOfColumnIncludingSentinels(columnName)),
                Limit.NO_LIMIT);
    }

    public static SlicePredicate create(Range range, Limit limit) {
        SliceRange slice = new SliceRange(
                range.start(),
                range.end(),
                false,
                limit.value());

        SlicePredicate predicate = new SlicePredicate();
        predicate.setSlice_range(slice);

        return predicate;
    }

    @Value.Immutable
    public interface Range {

        ByteBuffer UNBOUND_START = ByteBuffer.wrap(PtBytes.EMPTY_BYTE_ARRAY);
        ByteBuffer UNBOUND_END = ByteBuffer.wrap(PtBytes.EMPTY_BYTE_ARRAY);

        Range ALL = Range.of(UNBOUND_START, UNBOUND_END);

        static Range singleColumn(byte[] columnName, long maxTimestampExclusive) {
            ByteBuffer start = startOfColumn(columnName, maxTimestampExclusive);
            ByteBuffer end = endOfColumnIncludingSentinels(columnName);
            return of(start, end);
        }

        static ByteBuffer startOfColumn(byte[] columnName, long maxTimestampExclusive) {
            return CassandraKeyValueServices.makeCompositeBuffer(
                    columnName,
                    maxTimestampExclusive - 1);
        }

        static ByteBuffer endOfColumnIncludingSentinels(byte[] columnName) {
            return CassandraKeyValueServices.makeCompositeBuffer(
                    columnName,
                    -1);
        }

        static ByteBuffer endOfColumnExcludingSentinels(byte[] columnName) {
            return CassandraKeyValueServices.makeCompositeBuffer(
                    columnName,
                    0);
        }

        static Range of(ByteBuffer startInclusive, ByteBuffer endInclusive) {
            return ImmutableRange.builder().start(startInclusive).end(endInclusive).build();
        }

        ByteBuffer start();
        ByteBuffer end();
    }

    @Value.Immutable
    public interface Limit {

        Limit ZERO = Limit.of(0);
        Limit ONE = Limit.of(1);
        Limit NO_LIMIT = Limit.of(Integer.MAX_VALUE);

        static Limit of(int limit) {
            return ImmutableLimit.builder().value(limit).build();
        }

        int value();
    }

}
