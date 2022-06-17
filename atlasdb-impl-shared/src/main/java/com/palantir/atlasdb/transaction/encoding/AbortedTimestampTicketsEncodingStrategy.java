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

// PROTOTYPE: This is a copypasta of TicketsEncodingStrategy, with some small changes
public enum AbortedTimestampTicketsEncodingStrategy implements CellEncodingStrategy {
    INSTANCE;

    // DO NOT change the following without a transactions table migration!
    public static final long PARTITIONING_QUANTUM = 50_000_000;
    public static final int ROWS_PER_QUANTUM = 31;

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

        return (rowComponent / ROWS_PER_QUANTUM) * PARTITIONING_QUANTUM
                + columnComponent * ROWS_PER_QUANTUM
                + rowComponent % ROWS_PER_QUANTUM;
    }

    private static byte[] encodeRowName(long startTimestamp) {
        return rowToBytes(startTimestampToRow(startTimestamp));
    }

    private static long startTimestampToRow(long startTimestamp) {
        return (startTimestamp / PARTITIONING_QUANTUM) * ROWS_PER_QUANTUM
                + (startTimestamp % PARTITIONING_QUANTUM) % ROWS_PER_QUANTUM;
    }

    private static byte[] rowToBytes(long row) {
        return PtBytes.toBytes(Long.reverse(row));
    }

    private static byte[] encodeColumnName(long startTimestamp) {
        long column = (startTimestamp % PARTITIONING_QUANTUM) / ROWS_PER_QUANTUM;
        return ValueType.VAR_LONG.convertFromJava(column);
    }

    private static long decodeRowName(byte[] rowName) {
        return Long.reverse(PtBytes.toLong(rowName));
    }

    private static long decodeColumnName(byte[] columnName) {
        return (long) ValueType.VAR_LONG.convertToJava(columnName, 0);
    }
}
