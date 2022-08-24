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

import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * We divide the first PARTITIONING_QUANTUM timestamps among the first ROW_PER_QUANTUM rows.
 * We aim to distribute start timestamps as evenly as possible among these rows as numbers increase, by taking the
 * least significant bits of the timestamp and using that as the row number. For example, we would store values that
 * might be associated with the timestamps 1, ROW_PER_QUANTUM + 1, 2 * ROW_PER_QUANTUM + 1 etc. in the same row.
 *
 * One may view the layout of cells as follows (for example, where R is the number of rows per quantum and Q is the
 * partitioning quantum; N = Q/R):
 *
 *  row \ column |   0     1     2     3     ...      N-1
 *  -------------+----------------------------------------
 *             0 |   0     R    2R    3R     ...   (N-1)R
 *             1 |   1   R+1  2R+1  3R+1     ... (N-1)R+1
 *             2 |   2   R+2  2R+2  3R+2     ... (N-1)R+2
 *           ... |  ...  ...   ...   ...     ...   ...
 *           R-1 | R-1  2R-1  3R-1  4R-1     ...     NR-1
 *
 * We store the row name as a bit-wise reversed version of the row number to ensure even distribution in key-value
 * services that rely on consistent hashing or similar mechanisms for partitioning.
 */
public class TicketsCellEncodingStrategy implements CellEncodingStrategy {
    private final long partitioningQuantum;
    private final long rowsPerQuantum;

    public TicketsCellEncodingStrategy(long partitioningQuantum, long rowsPerQuantum) {
        Preconditions.checkArgument(
                rowsPerQuantum > 0,
                "Number of rows per quantum must be positive",
                SafeArg.of("rowsPerQuantum", rowsPerQuantum));
        Preconditions.checkArgument(
                partitioningQuantum >= rowsPerQuantum,
                "Must have at least one cell per row after partitioning",
                SafeArg.of("rowsPerQuantum", rowsPerQuantum),
                SafeArg.of("partitioningQuantum", partitioningQuantum));
        this.partitioningQuantum = partitioningQuantum;
        this.rowsPerQuantum = rowsPerQuantum;
    }

    @VisibleForTesting
    long getPartitioningQuantum() {
        return partitioningQuantum;
    }

    @VisibleForTesting
    long getRowsPerQuantum() {
        return rowsPerQuantum;
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
        long startRow = startTimestampToRow(fromInclusive - ((fromInclusive % partitioningQuantum) % rowsPerQuantum));
        long endRow = startTimestampToRow(
                toInclusive + rowsPerQuantum - ((toInclusive % partitioningQuantum) % rowsPerQuantum) - 1);
        return LongStream.rangeClosed(startRow, endRow).mapToObj(TicketsCellEncodingStrategy::rowToBytes);
    }

    /**
     * Returns a column range *covering* the provided timestamp range: notice that this may load more timestamps than
     * necessary. Users must post-filter any results they receive.
     */
    public ColumnRangeSelection getColumnRangeCoveringTimestampRange(long fromInclusive, long toInclusive) {
        long startPartition = getPartition(fromInclusive);
        long endPartition = getPartition(toInclusive);
        if (startPartition == endPartition) {
            return getQueryForSingleDoublyBoundedPartition(fromInclusive, toInclusive);
        }

        // The request covers more than one partition. In this case, there will not exist a suitable contiguous range
        // of columns containing all values, hence we just load all rows.
        return new ColumnRangeSelection(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY);
    }

    private ColumnRangeSelection getQueryForSingleDoublyBoundedPartition(long fromInclusive, long toInclusive) {
        long lowestRequiredColumn = getColumnIndex(fromInclusive);
        long highestRequiredColumn = getColumnIndex(toInclusive);

        return new ColumnRangeSelection(
                ValueType.VAR_LONG.convertFromJava(lowestRequiredColumn),
                ValueType.VAR_LONG.convertFromJava(getExclusiveUpperBound(highestRequiredColumn)));
    }

    private long getExclusiveUpperBound(long lastColumnInPartition) {
        return lastColumnInPartition + 1;
    }

    private long getPartition(long timestamp) {
        return timestamp / partitioningQuantum;
    }

    private long getColumnIndex(long timestamp) {
        return (timestamp % partitioningQuantum) / rowsPerQuantum;
    }
}
