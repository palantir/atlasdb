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

package com.palantir.atlasdb.keyvalue.cassandra;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.IntConsumer;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;

@SuppressWarnings("unchecked") // AssertJ assertions
public class CellLoadingBatcherTest {
    private static final int CROSS_COLUMN_LIMIT = 3;
    private static final int SINGLE_QUERY_LIMIT = 10;

    private IntConsumer rebatchingCallback = mock(IntConsumer.class);
    private CellLoadingBatcher batcher = new CellLoadingBatcher(
            () -> CROSS_COLUMN_LIMIT, () -> SINGLE_QUERY_LIMIT, rebatchingCallback);

    @Test
    public void splitsCellsByColumnKey() {
        List<Cell> cells = new ArrayList<>();
        cells.addAll(rowRange(CROSS_COLUMN_LIMIT, 0));
        cells.addAll(rowRange(CROSS_COLUMN_LIMIT, 1));

        List<List<Cell>> batches = batcher.partitionIntoBatches(cells);
        assertBatchContentsMatch(batches, rowRange(CROSS_COLUMN_LIMIT, 0), rowRange(CROSS_COLUMN_LIMIT, 1));
    }

    @Test
    public void doesNotSplitCellsForSingleColumnsIfUnderSingleQueryLimit() {
        List<List<Cell>> batches = batcher.partitionIntoBatches(rowRange(SINGLE_QUERY_LIMIT, 0));
        assertBatchContentsMatch(batches, rowRange(SINGLE_QUERY_LIMIT, 0));
    }

    @Test
    public void rebatchesLargeColumnsAndInvokesCallback() {
        List<List<Cell>> batches = batcher.partitionIntoBatches(rowRange(2 * SINGLE_QUERY_LIMIT, 0));
        assertBatchContentsMatch(batches,
                rowRange(0, SINGLE_QUERY_LIMIT, 0),
                rowRange(SINGLE_QUERY_LIMIT, 2 * SINGLE_QUERY_LIMIT, 0));
        verify(rebatchingCallback).accept(2 * SINGLE_QUERY_LIMIT);
    }

    @Test
    public void combinesSmallNumberOfRowsForDifferentColumns() {
        List<Cell> smallColumnBatch = ImmutableList.of(cell(1, 1), cell(1, 2), cell(2, 1));
        List<List<Cell>> batches = batcher.partitionIntoBatches(smallColumnBatch);
        assertBatchContentsMatch(batches, smallColumnBatch);
        verify(rebatchingCallback, never()).accept(anyInt());
    }

    @Test
    public void preservesCellsAcrossColumnsAndRebatchesThemToCrossColumnLimit() {
        List<Cell> manyColumns = columnRange(0, 0, 100);
        List<List<Cell>> batches = batcher.partitionIntoBatches(manyColumns);

        assertBatchContainsAllCells(batches, manyColumns);
        batches.forEach(batch -> assertThat(batch.size()).isLessThanOrEqualTo(CROSS_COLUMN_LIMIT));
    }

    @Test
    public void simultaneouslyHandlesLargeColumnsAndSmallColumns() {
        List<Cell> cells = new ArrayList<>();
        cells.addAll(rowRange(SINGLE_QUERY_LIMIT, 0));
        cells.addAll(rowRange(2 * SINGLE_QUERY_LIMIT, 1));
        cells.addAll(columnRange(0, 2, 2 + CROSS_COLUMN_LIMIT));

        List<List<Cell>> batches = batcher.partitionIntoBatches(cells);
        assertBatchContentsMatch(batches,
                rowRange(SINGLE_QUERY_LIMIT, 0),
                rowRange(0, SINGLE_QUERY_LIMIT, 1),
                rowRange(SINGLE_QUERY_LIMIT, 2 * SINGLE_QUERY_LIMIT, 1),
                columnRange(0, 2, 2 + CROSS_COLUMN_LIMIT));
        verify(rebatchingCallback).accept(2 * SINGLE_QUERY_LIMIT);
    }

    private static void assertBatchContentsMatch(List<List<Cell>> actual, List<Cell>... expected) {
        List<List<Cell>> sortedExpected = Arrays.stream(expected)
                .map(CellLoadingBatcherTest::sortedCopyOf)
                .collect(Collectors.toList());
        assertThat(actual)
                .extracting(CellLoadingBatcherTest::sortedCopyOf)
                .containsExactlyInAnyOrderElementsOf(sortedExpected);
    }

    private static void assertBatchContainsAllCells(List<List<Cell>> actual, List<Cell> expected) {
        assertThat(actual.stream().flatMap(Collection::stream)).hasSameElementsAs(expected);
    }

    private static List<Cell> sortedCopyOf(List<Cell> original) {
        return original.stream().sorted().collect(Collectors.toList());
    }

    private static Cell cell(long row, long column) {
        return Cell.create(PtBytes.toBytes(row), PtBytes.toBytes(column));
    }

    private static List<Cell> rowRange(long numRows, long column) {
        return rowRange(0, numRows, column);
    }

    private static List<Cell> rowRange(long firstRow, long lastRowExclusive, long column) {
        return LongStream.range(firstRow, lastRowExclusive)
                .mapToObj(row -> cell(row, column))
                .collect(Collectors.toList());
    }

    private static List<Cell> columnRange(long row, long firstColumn, long lastColumnExclusive) {
        return LongStream.range(firstColumn, lastColumnExclusive)
                .mapToObj(col -> cell(row, col))
                .collect(Collectors.toList());
    }
}
