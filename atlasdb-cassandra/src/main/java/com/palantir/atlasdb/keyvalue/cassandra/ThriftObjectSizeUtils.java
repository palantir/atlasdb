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

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.CounterColumn;
import org.apache.cassandra.thrift.CounterSuperColumn;
import org.apache.cassandra.thrift.SuperColumn;

public class ThriftObjectSizeUtils {

    private ThriftObjectSizeUtils() {
        // utility class
    }

    public static long getSizeOfColumnOrSuperColumn(ColumnOrSuperColumn columnOrSuperColumn) {
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
        return getByteArraySize(byteBuffer.array());
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
        return Long.SIZE;
    }
}
