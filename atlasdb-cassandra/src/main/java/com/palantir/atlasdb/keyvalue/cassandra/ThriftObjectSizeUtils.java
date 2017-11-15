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
import java.util.Map;
import java.util.function.Function;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.CounterColumn;
import org.apache.cassandra.thrift.CounterSuperColumn;
import org.apache.cassandra.thrift.CqlMetadata;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.cassandra.thrift.Deletion;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.SuperColumn;

public final class ThriftObjectSizeUtils {

    private static final int ONE_BYTE = 1;

    private ThriftObjectSizeUtils() {
        // utility class
    }

    public static int getColumnOrSuperColumnSize(ColumnOrSuperColumn columnOrSuperColumn) {
        if (columnOrSuperColumn == null) {
            return getNullSize();
        }
        return getColumnSize(columnOrSuperColumn.getColumn())
                + getSuperColumnSize(columnOrSuperColumn.getSuper_column())
                + getCounterColumnSize(columnOrSuperColumn.getCounter_column())
                + getCounterSuperColumnSize(columnOrSuperColumn.getCounter_super_column());
    }

    public static int getByteBufferSize(ByteBuffer byteBuffer) {
        if (byteBuffer == null) {
            return getNullSize();
        }
        return byteBuffer.remaining();
    }

    public static int getMutationSize(Mutation mutation) {
        if (mutation == null) {
            return getNullSize();
        }

        return getColumnOrSuperColumnSize(mutation.getColumn_or_supercolumn()) + getDeletionSize(
                mutation.getDeletion());
    }

    public static int getCqlResultSize(CqlResult cqlResult) {
        if (cqlResult == null) {
            return getNullSize();
        }
        return getThriftEnumSize()
                + getCollectionSize(cqlResult.getRows(), ThriftObjectSizeUtils::getCqlRowSize)
                + Integer.BYTES
                + getCqlMetadataSize(cqlResult.getSchema());
    }

    public static int getKeySliceSize(KeySlice keySlice) {
        if (keySlice == null) {
            return getNullSize();
        }

        return getByteArraySize(keySlice.getKey())
                + getCollectionSize(keySlice.getColumns(), ThriftObjectSizeUtils::getColumnOrSuperColumnSize);
    }

    public static int getStringSize(String string) {
        if (string == null) {
            return getNullSize();
        }

        return string.length() * Character.SIZE;
    }

    public static int getColumnSize(Column column) {
        if (column == null) {
            return getNullSize();
        }

        return getByteArraySize(column.getValue())
                + getByteArraySize(column.getName())
                + getTtlSize()
                + getTimestampSize();
    }

    private static int getCounterSuperColumnSize(CounterSuperColumn counterSuperColumn) {
        if (counterSuperColumn == null) {
            return getNullSize();
        }

        return getByteArraySize(counterSuperColumn.getName())
                + getCollectionSize(counterSuperColumn.getColumns(), ThriftObjectSizeUtils::getCounterColumnSize);
    }

    private static int getCounterColumnSize(CounterColumn counterColumn) {
        if (counterColumn == null) {
            return getNullSize();
        }

        return getByteArraySize(counterColumn.getName()) + getCounterValueSize();
    }

    private static int getSuperColumnSize(SuperColumn superColumn) {
        if (superColumn == null) {
            return getNullSize();
        }

        return getByteArraySize(superColumn.getName())
                + getCollectionSize(superColumn.getColumns(), ThriftObjectSizeUtils::getColumnSize);
    }

    private static int getDeletionSize(Deletion deletion) {
        if (deletion == null) {
            return getNullSize();
        }

        return getTimestampSize()
                + getByteArraySize(deletion.getSuper_column())
                + getSlicePredicateSize(deletion.getPredicate());
    }

    private static int getSlicePredicateSize(SlicePredicate predicate) {
        if (predicate == null) {
            return getNullSize();
        }

        return getCollectionSize(predicate.getColumn_names(), ThriftObjectSizeUtils::getByteBufferSize)
                + getSliceRangeSize(predicate.getSlice_range());
    }

    private static int getSliceRangeSize(SliceRange sliceRange) {
        if (sliceRange == null) {
            return getNullSize();
        }

        return getByteArraySize(sliceRange.getStart())
                + getByteArraySize(sliceRange.getFinish())
                + getReversedBooleanSize()
                + getSliceRangeCountSize();
    }

    private static int getCqlMetadataSize(CqlMetadata schema) {
        if (schema == null) {
            return getNullSize();
        }

        return getByteBufferStringMapSize(schema.getName_types())
                + getByteBufferStringMapSize(schema.getValue_types())
                + getStringSize(schema.getDefault_name_type())
                + getStringSize(schema.getDefault_value_type());
    }

    private static int getByteBufferStringMapSize(Map<ByteBuffer, String> nameTypes) {
        return getCollectionSize(nameTypes.entrySet(),
                entry -> ThriftObjectSizeUtils.getByteBufferSize(entry.getKey())
                        + ThriftObjectSizeUtils.getStringSize(entry.getValue()));
    }

    private static int getCqlRowSize(CqlRow cqlRow) {
        if (cqlRow == null) {
            return getNullSize();
        }
        return getByteArraySize(cqlRow.getKey())
                + getCollectionSize(cqlRow.getColumns(), ThriftObjectSizeUtils::getColumnSize);
    }

    private static int getThriftEnumSize() {
        return Integer.BYTES;
    }

    private static int getByteArraySize(byte[] byteArray) {
        if (byteArray == null) {
            return getNullSize();
        }
        return byteArray.length;
    }

    private static int getTimestampSize() {
        return Long.BYTES;
    }

    private static int getTtlSize() {
        return Integer.BYTES;
    }

    private static int getCounterValueSize() {
        return Long.BYTES;
    }

    private static int getReversedBooleanSize() {
        return ONE_BYTE;
    }

    private static int getSliceRangeCountSize() {
        return Integer.BYTES;
    }

    private static int getNullSize() {
        return Integer.BYTES;
    }

    public static <T> int getCollectionSize(Collection<T> collection, Function<T, Integer> sizeFunction) {
        if (collection == null) {
            return getNullSize();
        }

        int sum = 0;
        for (T item : collection) {
            sum += sizeFunction.apply(item);
        }
        return sum;
    }
}
