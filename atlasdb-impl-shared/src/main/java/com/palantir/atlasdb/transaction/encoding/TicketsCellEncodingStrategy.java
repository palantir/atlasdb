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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.immutables.value.Value;

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

    public CellRangeQuery getRangeQueryCoveringTimestampRange(long fromInclusive, long toInclusive) {
        long startPartition = getPartition(fromInclusive);
        long endPartition = getPartition(toInclusive);
        if (startPartition == endPartition) {
            return getQueryForSingleDoublyBoundedPartition(startPartition, fromInclusive, toInclusive);
        }
        return getRangeQueryCoveringMultiplePartitions(fromInclusive, toInclusive, startPartition, endPartition);
    }

    private CellRangeQuery getRangeQueryCoveringMultiplePartitions(
            long fromInclusive,
            long toInclusive,
            long startPartition,
            long endPartition) {
        Preconditions.checkState(endPartition > startPartition,
                "Expecting query to cross multiple partitions");
        List<CellRangeQuery> individualPartitionQueries = new ArrayList<>();
        individualPartitionQueries.add(getRangeQueryCoveringTimestampRange(fromInclusive,
                getEndTimestampForPartition(startPartition)));
        for (long currentPartition = getExclusiveUpperBound(startPartition); currentPartition < endPartition; currentPartition++) {
            individualPartitionQueries.add(getRangeQueryCoveringTimestampRange(getStartTimestampForPartition(currentPartition), getEndTimestampForPartition(currentPartition)));
        }
        individualPartitionQueries.add(getRangeQueryCoveringTimestampRange(getStartTimestampForPartition(endPartition), toInclusive));
        return CellRangeQuery.merge(individualPartitionQueries);
    }

    private CellRangeQuery getQueryForSingleDoublyBoundedPartition(
            long partitionNumber, long fromInclusive, long toInclusive) {
        ImmutableCellRangeQuery.Builder builder = ImmutableCellRangeQuery.builder();
        for (int rowOffset = 0; rowOffset < rowsPerQuantum; rowOffset++) {
            long firstColumnInPartition = getFirstColumnInPartition(fromInclusive, rowOffset);
            long lastColumnInPartition = getLastColumnInPartition(toInclusive, rowOffset);
            if (firstColumnInPartition <= lastColumnInPartition) {
                builder.putRowsToBeLoaded(
                        ValueType.VAR_LONG.convertFromJava(partitionNumber * rowsPerQuantum + rowOffset),
                        new ColumnRangeSelection(
                                ValueType.VAR_LONG.convertFromJava(firstColumnInPartition),
                                ValueType.VAR_LONG.convertFromJava(getExclusiveUpperBound(lastColumnInPartition))));
            }
        }
        return builder.build();
    }

    private long getExclusiveUpperBound(long lastColumnInPartition) {
        return lastColumnInPartition + 1;
    }

    /**
     * Returns the first column in a given row that would be relevant given the lower bound of the timestamp range.
     * For example, where rowsPerQuantum = 10,
     * - getFirstColumnInPartition(75, 5) = 7, because in row 5 the cell with column 7 represents timestamp 75.
     * - getFirstColumnInPartition(75, 4) = 8, because in row 4, the cell with column 7 represents timestamp 74
     * (which is less than 75), but the cell with column 8 represents timestamp 84 (greater than 75).
     */
    private long getFirstColumnInPartition(long fromInclusive, int rowOffset) {
        return getPartitionOffsetRow(fromInclusive) <= rowOffset
                ? getColumnIndex(fromInclusive)
                : getColumnIndex(fromInclusive) + 1;
    }

    /**
     * Returns the last column in a given row that would be relevant given the lower bound of the timestamp range.
     * For example, where rowsPerQuantum = 10,
     * - getLastColumnInPartition(75, 5) = 7, because in row 5 the cell with column 7 represents timestamp 75.
     * - getLastColumnInPartition(75, 6) = 6, because in row 6, the cell with column 7 represents timestamp 76
     * (which is greater than 75), but the cell with column 6 represents timestamp 66 (less than 75).
     */
    private long getLastColumnInPartition(long toInclusive, int rowOffset) {
        return getPartitionOffsetRow(toInclusive) >= rowOffset
                ? getColumnIndex(toInclusive)
                : getColumnIndex(toInclusive) - 1;
    }

    private long getPartition(long timestamp) {
        return timestamp / partitioningQuantum;
    }

    private long getStartTimestampForPartition(long partition) {
        return partition * partitioningQuantum;
    }

    private long getEndTimestampForPartition(long partition) {
        return (partition + 1) * partitioningQuantum - 1;
    }

    private long getColumnIndex(long timestamp) {
        return (timestamp % partitioningQuantum) / rowsPerQuantum;
    }

    private long getPartitionOffsetRow(long timestamp) {
        return (timestamp % partitioningQuantum) % rowsPerQuantum;
    }

    @Value.Immutable
    interface CellRangeQuery {
        Map<byte[], ColumnRangeSelection> rowsToBeLoaded();

        static CellRangeQuery merge(List<CellRangeQuery> queries) {
            ImmutableCellRangeQuery.Builder builder = ImmutableCellRangeQuery.builder();
            queries.forEach(query -> builder.putAllRowsToBeLoaded(query.rowsToBeLoaded()));
            return builder.build();
        }
    }
}
