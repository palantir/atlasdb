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

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.CounterColumn;
import org.apache.cassandra.thrift.CounterSuperColumn;
import org.apache.cassandra.thrift.Deletion;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.SuperColumn;

public class ThriftObjectSizeUtils {

    private static final long ONE_BYTE = 1L;

    private ThriftObjectSizeUtils() {
        // utility class
    }

    public static long getColumnOrSuperColumnSize(ColumnOrSuperColumn columnOrSuperColumn) {
        return getColumnSize(columnOrSuperColumn.getColumn())
                + getSuperColumnSize(columnOrSuperColumn.getSuper_column())
                + getCounterColumnSize(columnOrSuperColumn.getCounter_column())
                + getCounterSuperColumnSize(columnOrSuperColumn.getCounter_super_column());
    }

    private static long getCounterSuperColumnSize(CounterSuperColumn counterSuperColumn) {
        return getByteArraySize(counterSuperColumn.getName()) +
                counterSuperColumn.getColumns().stream()
                        .mapToLong(ThriftObjectSizeUtils::getCounterColumnSize)
                        .sum();
    }

    public static long getCounterColumnSize(CounterColumn counterColumn) {
        return getByteArraySize(counterColumn.getName()) + getCounterValueSize();
    }

    public static long getSuperColumnSize(SuperColumn superColumn) {
        return getByteBufferSize(superColumn.name) +
                superColumn.getColumns().stream()
                        .mapToLong(ThriftObjectSizeUtils::getColumnSize)
                        .sum();
    }

    public static long getByteBufferSize(ByteBuffer byteBuffer) {
        // Position is the size unless something has been read from the ByteBuffer
        return byteBuffer.position();
    }

    public static long getByteArraySize(byte[] byteArray) {
        return byteArray.length;
    }

    public static long getColumnSize(Column column) {
        return column.getValue().length + column.getName().length + getTtlSize() + getTimestampSize();
    }

    public static long getTimestampSize() {
        return Long.BYTES;
    }

    public static long getTtlSize() {
        return Integer.BYTES;
    }

    public static long getCounterValueSize() {
        return Long.BYTES;
    }

    public static long getMutationSize(Mutation mutation) {
        return getColumnOrSuperColumnSize(mutation.getColumn_or_supercolumn()) + getDeletionSize(mutation.getDeletion());
    }

    public static long getDeletionSize(Deletion deletion) {
        return getTimestampSize()
                + getByteArraySize(deletion.getSuper_column())
                + getSlicePredicateSize(deletion.getPredicate());
    }

    private static long getSlicePredicateSize(SlicePredicate predicate) {
        return getByteBufferCollectionSize(predicate.getColumn_names()) + getSliceRangeSize(predicate.getSlice_range());
    }

    private static long getSliceRangeSize(SliceRange sliceRange) {
        return getByteArraySize(sliceRange.getStart())
                + getByteArraySize(sliceRange.getFinish())
                + getReversedBooleanSize()
                + getSliceRangeCountSize();
    }

    private static long getReversedBooleanSize() {
        return ONE_BYTE;
    }

    private static int getSliceRangeCountSize() {
        return Integer.BYTES;
    }

    public static long getByteBufferCollectionSize(Collection<ByteBuffer> byteBufferList) {
        return byteBufferList.stream()
                .mapToLong(ThriftObjectSizeUtils::getByteBufferSize)
                .sum();
    }
}
