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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.junit.Test;

public class TicketsCellEncodingStrategyTest {
    private final long SMALL_QUANTUM = 100L;
    private final long SMALL_NUMBER_OF_ROWS = 5L;
    private final TicketsCellEncodingStrategy SMALL_QUANTUM_STRATEGY =
            new TicketsCellEncodingStrategy(SMALL_QUANTUM, SMALL_NUMBER_OF_ROWS);

    private final long LARGE_QUANTUM = 1_000_000_000L;
    private final long LARGE_NUMBER_OF_ROWS = 1_000L;
    private final TicketsCellEncodingStrategy LARGE_QUANTUM_STRATEGY =
            new TicketsCellEncodingStrategy(LARGE_QUANTUM, LARGE_NUMBER_OF_ROWS);

    private final Set<TicketsCellEncodingStrategy> STRATEGIES =
            ImmutableSet.of(SMALL_QUANTUM_STRATEGY, LARGE_QUANTUM_STRATEGY);

    @Test
    public void partitioningParametersMustBePositive() {
        assertThatThrownBy(() -> new TicketsCellEncodingStrategy(0L, 0L))
                .isInstanceOf(SafeIllegalArgumentException.class)
                .hasMessageContaining("Number of rows per quantum must be positive");
        assertThatThrownBy(() -> new TicketsCellEncodingStrategy(10L, 0L))
                .isInstanceOf(SafeIllegalArgumentException.class)
                .hasMessageContaining("Number of rows per quantum must be positive");
        assertThatThrownBy(() -> new TicketsCellEncodingStrategy(10L, -5L))
                .isInstanceOf(SafeIllegalArgumentException.class)
                .hasMessageContaining("Number of rows per quantum must be positive");
        assertThatThrownBy(() -> new TicketsCellEncodingStrategy(0L, 10L))
                .isInstanceOf(SafeIllegalArgumentException.class)
                .hasMessageContaining("Must have at least one cell per row after partitioning");
    }

    @Test
    public void mustHaveAtLeastAsManyRowsPerPartitioningQuantum() {
        assertThatCode(() -> new TicketsCellEncodingStrategy(50L, 50L)).doesNotThrowAnyException();
        assertThatThrownBy(() -> new TicketsCellEncodingStrategy(50L, 51L))
                .isInstanceOf(SafeIllegalArgumentException.class)
                .hasMessageContaining("Must have at least one cell per row after partitioning");
    }

    @Test
    public void encodingAndDecodingTimestampsIsIdempotent() {
        STRATEGIES.forEach(strategy -> {
            ImmutableList<Long> testCases = ImmutableList.of(
                    1L,
                    strategy.getPartitioningQuantum() / 2,
                    strategy.getPartitioningQuantum() - 1,
                    strategy.getPartitioningQuantum(),
                    strategy.getPartitioningQuantum() + 1,
                    3 * strategy.getPartitioningQuantum() - 1,
                    4 * strategy.getPartitioningQuantum(),
                    5 * strategy.getPartitioningQuantum() + 1);
            testCases.forEach(testCase -> assertThat(
                            strategy.decodeCellAsStartTimestamp(strategy.encodeStartTimestampAsCell(testCase)))
                    .isEqualTo(testCase));
        });
    }

    @Test
    public void valuesSpacedNumRowsApartAreInTheSamePartition() {
        long timestamp = 3;
        STRATEGIES.forEach(strategy -> {
            Cell baseCell = strategy.encodeStartTimestampAsCell(timestamp);
            Cell numRowsAddedCell = strategy.encodeStartTimestampAsCell(timestamp + strategy.getRowsPerQuantum());
            Cell threeTimesNumRowsAddedCell =
                    strategy.encodeStartTimestampAsCell(timestamp + 3 * strategy.getRowsPerQuantum());
            assertThat(baseCell.getRowName()).isEqualTo(numRowsAddedCell.getRowName());
            assertThat(baseCell.getColumnName()).isNotEqualTo(numRowsAddedCell.getColumnName());

            assertThat(baseCell.getRowName()).isEqualTo(threeTimesNumRowsAddedCell.getRowName());
            assertThat(baseCell.getColumnName()).isNotEqualTo(threeTimesNumRowsAddedCell.getColumnName());
        });
    }

    @Test
    public void getRowNamesRangeDrawsFromCorrectPartitions() {
        long lowerBound = 3 * LARGE_QUANTUM_STRATEGY.getPartitioningQuantum() - 3;
        long upperBound = 17 * LARGE_QUANTUM_STRATEGY.getPartitioningQuantum() + 1;

        List<Long> rowsReturned = LARGE_QUANTUM_STRATEGY
                .getRowSetCoveringTimestampRange(lowerBound, upperBound)
                .map(PtBytes::toLong)
                .map(Long::reverse)
                .collect(Collectors.toList());
        // We could potentially be smarter here (e.g. technically only the first two rows in the 18th block of the
        // large quantum strategy are required), but we don't currently do that.
        assertThat(rowsReturned)
                .hasSameElementsAs(LongStream.range(
                                2 * LARGE_QUANTUM_STRATEGY.getRowsPerQuantum(),
                                18 * LARGE_QUANTUM_STRATEGY.getRowsPerQuantum())
                        .boxed()
                        .collect(Collectors.toList()));
    }

    @Test
    public void columnRangeForMultiPartitionQueriesIsUniversal() {
        assertThat(SMALL_QUANTUM_STRATEGY.getColumnRangeCoveringTimestampRange(SMALL_QUANTUM * 3, SMALL_QUANTUM * 4))
                .isEqualTo(new ColumnRangeSelection(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY));
        assertThat(SMALL_QUANTUM_STRATEGY.getColumnRangeCoveringTimestampRange(
                        SMALL_QUANTUM * 17, SMALL_QUANTUM * 88 - 1))
                .isEqualTo(new ColumnRangeSelection(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY));
    }

    @Test
    public void columnRangeForSingleBoundedFullPartition() {
        ColumnRangeSelection selection =
                SMALL_QUANTUM_STRATEGY.getColumnRangeCoveringTimestampRange(0, SMALL_QUANTUM - 1);
        assertThat(decode(selection)).isEqualTo(Range.closedOpen(0L, SMALL_QUANTUM / SMALL_NUMBER_OF_ROWS));
    }

    @Test
    public void columnRangeForSingleBoundedSingleValue() {
        long timestamp = 88L;
        ColumnRangeSelection selection =
                SMALL_QUANTUM_STRATEGY.getColumnRangeCoveringTimestampRange(timestamp, timestamp);
        assertThat(decode(selection))
                .isEqualTo(Range.closedOpen(timestamp / SMALL_NUMBER_OF_ROWS, timestamp / SMALL_NUMBER_OF_ROWS + 1));
    }

    @Test
    public void columnRangeForSingleBoundedContiguousRange() {
        ColumnRangeSelection selection = SMALL_QUANTUM_STRATEGY.getColumnRangeCoveringTimestampRange(
                3 * SMALL_NUMBER_OF_ROWS, 7 * SMALL_NUMBER_OF_ROWS - 1);
        assertThat(decode(selection)).isEqualTo(Range.closedOpen(3L, 7L));
    }

    @Test
    public void getRangeQueryForSingleBoundedPartitionWithWrapAroundRange() {
        ColumnRangeSelection selection = SMALL_QUANTUM_STRATEGY.getColumnRangeCoveringTimestampRange(
                6 * SMALL_NUMBER_OF_ROWS - 1, 6 * SMALL_NUMBER_OF_ROWS + 1);
        assertThat(decode(selection)).isEqualTo(Range.closedOpen(5L, 7L));
    }

    private static Range<Long> decode(ColumnRangeSelection columnRangeSelection) {
        return Range.closedOpen((Long) ValueType.VAR_LONG.convertToJava(columnRangeSelection.getStartCol(), 0), (Long)
                ValueType.VAR_LONG.convertToJava(columnRangeSelection.getEndCol(), 0));
    }
}
