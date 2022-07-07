/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.encoding;

import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.table.description.ValueType;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class TicketsCellEncodingStrategy implements CellEncodingStrategy {
    private final long partitioningQuantum;
    private final long rowsPerQuantum;

    public TicketsCellEncodingStrategy(long partitioningQuantum, long rowsPerQuantum) {
        this.partitioningQuantum = partitioningQuantum;
        this.rowsPerQuantum = rowsPerQuantum;
    }

    @Override
    public Cell encodeStartTimestampAsCell(long startTimestamp) {
        byte[] rowName = encodeRowName(startTimestamp);
        byte[] columnName = encodeColumnName(startTimestamp);
        return Cell.create(rowName, columnName);
    }

    @Override
    public long decodeCellAsStartTimestamp(Cell cell) {
        long rowComponent = decodeRowName(cell.getRowName());
        long columnComponent = decodeColumnName(cell.getColumnName());

        return (rowComponent / rowsPerQuantum) * partitioningQuantum
                + columnComponent * rowsPerQuantum
                + rowComponent % rowsPerQuantum;
    }

    private byte[] encodeRowName(long startTimestamp) {
        return rowToBytes(startTimestampToRow(startTimestamp));
    }

    private long startTimestampToRow(long startTimestamp) {
        return (startTimestamp / partitioningQuantum) * rowsPerQuantum
                + (startTimestamp % partitioningQuantum) % rowsPerQuantum;
    }

    private static byte[] rowToBytes(long row) {
        return PtBytes.toBytes(Long.reverse(row));
    }

    private byte[] encodeColumnName(long startTimestamp) {
        long column = (startTimestamp % partitioningQuantum) / rowsPerQuantum;
        return ValueType.VAR_LONG.convertFromJava(column);
    }

    private static long decodeRowName(byte[] rowName) {
        return Long.reverse(PtBytes.toLong(rowName));
    }

    private static long decodeColumnName(byte[] columnName) {
        return (long) ValueType.VAR_LONG.convertToJava(columnName, 0);
    }

    public Stream<byte[]> getRowSetCoveringTimestampRange(long fromInclusive, long toInclusive) {
        long startRow =
                startTimestampToRow(fromInclusive - ((fromInclusive % partitioningQuantum) % rowsPerQuantum));
        long endRow = startTimestampToRow(
                toInclusive + rowsPerQuantum - ((toInclusive % partitioningQuantum) % rowsPerQuantum) - 1);
        return LongStream.rangeClosed(startRow, endRow).mapToObj(TicketsCellEncodingStrategy::rowToBytes);
    }
}
