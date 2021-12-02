/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import java.util.Arrays;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * The ticketing algorithm distributes timestamps among rows and dynamic columns to avoid hot-spotting.
 *
 * We divide the first PARTITIONING_QUANTUM timestamps among the first ROW_PER_QUANTUM rows.
 * We aim to distribute start timestamps as evenly as possible among these rows as numbers increase, by taking the
 * least significant bits of the timestamp and using that as the row number. For example, we would store timestamps
 * 1, ROW_PER_QUANTUM + 1, 2 * ROW_PER_QUANTUM + 1 etc. in the same row.
 *
 * We store the row name as a bit-wise reversed version of the row number to ensure even distribution in key-value
 * services that rely on consistent hashing or similar mechanisms for partitioning.
 *
 * We also use a delta encoding for the commit timestamp as these differences are expected to be small.
 *
 * A long is 9 bytes at most, so one row here consists of at most 4 longs -> 36 bytes, and an individual row
 * is probabilistically below 1M dynamic column keys. A row is expected to be less than 14M (56M worst case).
 *
 * Note the usage of {@link PtBytes#EMPTY_BYTE_ARRAY} for transactions that were rolled back; this is a space
 * optimisation, as we would otherwise store a negative value which uses 9 bytes in a VAR_LONG.
 */
public enum TicketsEncodingStrategy implements TimestampEncodingStrategy<Long> {
    INSTANCE;

    public static final byte[] ABORTED_TRANSACTION_VALUE = PtBytes.EMPTY_BYTE_ARRAY;

    // DO NOT change the following without a transactions table migration!
    public static final long PARTITIONING_QUANTUM = 25_000_000;
    public static final int ROWS_PER_QUANTUM = TransactionConstants.V2_TRANSACTION_NUM_PARTITIONS;

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

    @Override
    public byte[] encodeCommitTimestampAsValue(long startTimestamp, Long commitTimestamp) {
        if (commitTimestamp == TransactionConstants.FAILED_COMMIT_TS) {
            return ABORTED_TRANSACTION_VALUE;
        }
        return TransactionConstants.getValueForTimestamp(commitTimestamp - startTimestamp);
    }

    @Override
    public Long decodeValueAsCommitTimestamp(long startTimestamp, byte[] value) {
        if (Arrays.equals(value, ABORTED_TRANSACTION_VALUE)) {
            return TransactionConstants.FAILED_COMMIT_TS;
        }
        return startTimestamp + TransactionConstants.getTimestampForValue(value);
    }

    private static byte[] encodeRowName(long startTimestamp) {
        return rowToBytes(startTimestampToRow(startTimestamp));
    }

    public Stream<byte[]> encodeRangeOfStartTimestampsAsRows(long fromInclusive, long toInclusive) {
        // todo(gmaretic): maybe calculate the exact rows, modal arithmetic is tedious and will be difficult to parse?
        long startRow =
                startTimestampToRow(fromInclusive - ((fromInclusive % PARTITIONING_QUANTUM) % ROWS_PER_QUANTUM));
        long endRow = startTimestampToRow(
                toInclusive + ROWS_PER_QUANTUM - ((toInclusive % PARTITIONING_QUANTUM) % ROWS_PER_QUANTUM) - 1);
        return LongStream.rangeClosed(startRow, endRow).mapToObj(TicketsEncodingStrategy::rowToBytes);
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
