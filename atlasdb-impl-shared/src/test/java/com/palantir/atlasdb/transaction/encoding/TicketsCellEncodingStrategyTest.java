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
import com.palantir.atlasdb.transaction.encoding.TicketsCellEncodingStrategy.CellRangeQuery;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import java.util.List;
import java.util.Map;
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
    private final long LARGE_QUANTUM_MAX_COLUMN = LARGE_QUANTUM / LARGE_NUMBER_OF_ROWS - 1;
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
    public void getRangeQueryForSingleBoundedPartitionWithCompleteRange() {
        assertThat(SMALL_QUANTUM_STRATEGY
                        .getRangeQueryCoveringTimestampRange(SMALL_QUANTUM * 3, SMALL_QUANTUM * 4 - 1)
                        .rowsToBeLoaded())
                .satisfies(map -> {
                    Map<Long, ColumnRangeSelection> columnToRangeSelection = KeyedStream.stream(map)
                            .mapKeys(rowKey -> (long) ValueType.VAR_LONG.convertToJava(rowKey, 0))
                            .collectToMap();

                    List<Long> expectedRows = LongStream.range(3 * SMALL_NUMBER_OF_ROWS, 4 * SMALL_NUMBER_OF_ROWS)
                            .boxed()
                            .collect(Collectors.toList());
                    assertThat(columnToRangeSelection.keySet()).hasSameElementsAs(expectedRows);
                    expectedRows.forEach(expectedRow -> assertThat(columnToRangeSelection.get(expectedRow)).satisfies(rangeSelection -> {
                        assertThat(ValueType.VAR_LONG.convertToJava(rangeSelection.getStartCol(), 0))
                                .isEqualTo(0L);
                        assertThat(ValueType.VAR_LONG.convertToJava(rangeSelection.getEndCol(), 0))
                                .isEqualTo(SMALL_QUANTUM / SMALL_NUMBER_OF_ROWS);
                    }));
                });
    }

    @Test
    public void getRangeQueryForSingleBoundedPartitionWithWrapAroundRange() {
        CellRangeQuery cellRangeQueryNoCompleteLoop = SMALL_QUANTUM_STRATEGY.getRangeQueryCoveringTimestampRange(9, 11);
        assertThat(cellRangeQueryNoCompleteLoop.rowsToBeLoaded()).satisfies(map -> {
            Map<Long, ColumnRangeSelection> columnToRangeSelection = KeyedStream.stream(map)
                    .mapKeys(rowKey -> (long) ValueType.VAR_LONG.convertToJava(rowKey, 0))
                    .collectToMap();
            ColumnRangeSelection offsetFourSelection = columnToRangeSelection.get(4L);
            assertThat(ValueType.VAR_LONG.convertToJava(offsetFourSelection.getStartCol(), 0)).isEqualTo(1L);
            assertThat(ValueType.VAR_LONG.convertToJava(offsetFourSelection.getEndCol(), 0)).isEqualTo(2L);

            ColumnRangeSelection offsetZeroSelection = columnToRangeSelection.get(0L);
            assertThat(ValueType.VAR_LONG.convertToJava(offsetZeroSelection.getStartCol(), 0)).isEqualTo(2L);
            assertThat(ValueType.VAR_LONG.convertToJava(offsetZeroSelection.getEndCol(), 0)).isEqualTo(3L);

            ColumnRangeSelection offsetOneSelection = columnToRangeSelection.get(1L);
            assertThat(ValueType.VAR_LONG.convertToJava(offsetOneSelection.getStartCol(), 0)).isEqualTo(2L);
            assertThat(ValueType.VAR_LONG.convertToJava(offsetOneSelection.getEndCol(), 0)).isEqualTo(3L);

            assertThat(columnToRangeSelection.keySet()).containsExactly(0L, 1L, 4L);
        });
    }

    @Test
    public void getRangeQueryAcrossMultiplePartitioningQuanta() {
        long lowerBound = 3 * LARGE_QUANTUM_STRATEGY.getPartitioningQuantum() - 3;
        long upperBound = 17 * LARGE_QUANTUM_STRATEGY.getPartitioningQuantum() + 1;

        CellRangeQuery query = LARGE_QUANTUM_STRATEGY.getRangeQueryCoveringTimestampRange(lowerBound, upperBound);
        Map<Long, ColumnRangeSelection> selections = KeyedStream.stream(query.rowsToBeLoaded())
                .mapKeys(bytes -> (Long) ValueType.VAR_LONG.convertToJava(bytes, 0))
                .collectToMap();

        Range<Long> lastColumnOnly = Range.closedOpen(LARGE_QUANTUM_MAX_COLUMN, LARGE_QUANTUM_MAX_COLUMN + 1);
        assertThat(selections).doesNotContainKey(3 * LARGE_QUANTUM_STRATEGY.getRowsPerQuantum() - 4);
        assertThat(decode(selections.get(3 * LARGE_QUANTUM_STRATEGY.getRowsPerQuantum() - 3))).isEqualTo(lastColumnOnly);
        assertThat(decode(selections.get(3 * LARGE_QUANTUM_STRATEGY.getRowsPerQuantum() - 2))).isEqualTo(lastColumnOnly);
        assertThat(decode(selections.get(3 * LARGE_QUANTUM_STRATEGY.getRowsPerQuantum() - 1))).isEqualTo(lastColumnOnly);

        for (long row = 3 * LARGE_QUANTUM_STRATEGY.getRowsPerQuantum(); row < 17 * LARGE_QUANTUM_STRATEGY.getRowsPerQuantum(); row++) {
            assertThat(decode(selections.get(row))).isEqualTo(Range.closedOpen(0L, LARGE_QUANTUM_MAX_COLUMN + 1));
        }

        Range<Long> firstColumnOnly = Range.closedOpen(0L, 1L);
        assertThat(decode(selections.get(17 * LARGE_QUANTUM_STRATEGY.getRowsPerQuantum()))).isEqualTo(firstColumnOnly);
        assertThat(decode(selections.get(17 * LARGE_QUANTUM_STRATEGY.getRowsPerQuantum() + 1))).isEqualTo(firstColumnOnly);
        assertThat(selections).doesNotContainKey(17 * LARGE_QUANTUM_STRATEGY.getRowsPerQuantum() + 2);
    }

    private static Range<Long> decode(ColumnRangeSelection columnRangeSelection) {
        return Range.closedOpen((Long) ValueType.VAR_LONG.convertToJava(columnRangeSelection.getStartCol(), 0),
                (Long) ValueType.VAR_LONG.convertToJava(columnRangeSelection.getEndCol(), 0));
    }
}
