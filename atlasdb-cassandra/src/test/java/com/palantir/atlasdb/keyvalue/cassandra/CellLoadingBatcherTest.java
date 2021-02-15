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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.cassandra.CassandraCellLoadingConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraCellLoadingConfig;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import org.junit.Test;

@SuppressWarnings("unchecked") // AssertJ assertions
public class CellLoadingBatcherTest {
    private static final int CROSS_COLUMN_LIMIT = 3;
    private static final int SINGLE_QUERY_LIMIT = 10;
    private static final CassandraCellLoadingConfig LOADING_CONFIG = ImmutableCassandraCellLoadingConfig.builder()
            .crossColumnLoadBatchLimit(CROSS_COLUMN_LIMIT)
            .singleQueryLoadBatchLimit(SINGLE_QUERY_LIMIT)
            .build();

    private static final InetSocketAddress ADDRESS = new InetSocketAddress(42);
    private static final TableReference TABLE_REFERENCE = TableReference.createFromFullyQualifiedName("a.b");
    private static final int SEED = 1;

    private CellLoadingBatcher.BatchCallback rebatchingCallback = mock(CellLoadingBatcher.BatchCallback.class);
    private CellLoadingBatcher batcher = new CellLoadingBatcher(() -> LOADING_CONFIG, rebatchingCallback);

    @Test
    public void handlesEmptyBatch() {
        List<List<Cell>> batches = partitionUsingMockCallback(ImmutableList.of());
        assertThat(batches).isEmpty();
    }

    @Test
    public void queriesOneCellInOneBatch() {
        List<Cell> oneCell = ImmutableList.of(cell(0, 0));
        List<List<Cell>> batches = partitionUsingMockCallback(oneCell);
        assertBatchContentsMatch(batches, oneCell);
    }

    @Test
    public void splitsCellsByColumnKey() {
        List<Cell> cells = new ArrayList<>();
        cells.addAll(rowRange(CROSS_COLUMN_LIMIT, 0));
        cells.addAll(rowRange(CROSS_COLUMN_LIMIT, 1));

        List<List<Cell>> batches = partitionUsingMockCallback(cells);
        assertBatchContentsMatch(batches, rowRange(CROSS_COLUMN_LIMIT, 0), rowRange(CROSS_COLUMN_LIMIT, 1));
    }

    @Test
    public void doesNotSplitCellsForSingleColumnsIfUnderSingleQueryLimit() {
        List<List<Cell>> batches = partitionUsingMockCallback(rowRange(SINGLE_QUERY_LIMIT, 0));
        assertBatchContentsMatch(batches, rowRange(SINGLE_QUERY_LIMIT, 0));
    }

    @Test
    public void rebatchesLargeColumnsAndInvokesCallback() {
        List<List<Cell>> batches = partitionUsingMockCallback(rowRange(2 * SINGLE_QUERY_LIMIT, 0));
        assertBatchContentsMatch(
                batches, rowRange(0, SINGLE_QUERY_LIMIT, 0), rowRange(SINGLE_QUERY_LIMIT, 2 * SINGLE_QUERY_LIMIT, 0));
        verify(rebatchingCallback).consume(ADDRESS, TABLE_REFERENCE, 2 * SINGLE_QUERY_LIMIT);
    }

    @Test
    public void combinesSmallNumberOfRowsForDifferentColumns() {
        List<Cell> smallColumnBatch = ImmutableList.of(cell(1, 1), cell(1, 2), cell(2, 1));
        List<List<Cell>> batches = partitionUsingMockCallback(smallColumnBatch);
        assertBatchContentsMatch(batches, smallColumnBatch);
        verify(rebatchingCallback, never()).consume(any(), any(), anyInt());
    }

    @Test
    public void preservesCellsAcrossColumnsAndRebatchesThemToCrossColumnLimit() {
        List<Cell> manyColumns = columnRange(0, 0, 100);
        List<List<Cell>> batches = partitionUsingMockCallback(manyColumns);

        assertBatchContainsAllCells(batches, manyColumns);
        batches.forEach(batch -> assertThat(batch).hasSizeLessThanOrEqualTo(CROSS_COLUMN_LIMIT));
    }

    @Test
    public void simultaneouslyHandlesLargeColumnsAndSmallColumns() {
        List<Cell> cells = new ArrayList<>();
        cells.addAll(rowRange(SINGLE_QUERY_LIMIT, 0));
        cells.addAll(rowRange(2 * SINGLE_QUERY_LIMIT, 1));
        cells.addAll(columnRange(0, 2, 2 + CROSS_COLUMN_LIMIT));

        List<List<Cell>> batches = partitionUsingMockCallback(cells);
        assertBatchContentsMatch(
                batches,
                rowRange(SINGLE_QUERY_LIMIT, 0),
                rowRange(0, SINGLE_QUERY_LIMIT, 1),
                rowRange(SINGLE_QUERY_LIMIT, 2 * SINGLE_QUERY_LIMIT, 1),
                columnRange(0, 2, 2 + CROSS_COLUMN_LIMIT));
        verify(rebatchingCallback).consume(ADDRESS, TABLE_REFERENCE, 2 * SINGLE_QUERY_LIMIT);
    }

    @Test
    public void respondsToChangesInCrossColumnLimitBatchParameter() {
        AtomicReference<CassandraCellLoadingConfig> config = new AtomicReference<>(LOADING_CONFIG);
        CellLoadingBatcher reloadingBatcher = new CellLoadingBatcher(config::get, rebatchingCallback);
        updateConfig(config, 2 * CROSS_COLUMN_LIMIT, SINGLE_QUERY_LIMIT);

        List<List<Cell>> largerBatches = reloadingBatcher.partitionIntoBatches(
                columnRange(0, 0, 2 * CROSS_COLUMN_LIMIT), ADDRESS, TABLE_REFERENCE);
        assertBatchContentsMatch(largerBatches, columnRange(0, 0, 2 * CROSS_COLUMN_LIMIT));
        verify(rebatchingCallback, never()).consume(any(), any(), anyInt());
    }

    private void updateConfig(
            AtomicReference<CassandraCellLoadingConfig> config, int crossColumnLimit, int singleQueryLimit) {
        config.set(ImmutableCassandraCellLoadingConfig.builder()
                .crossColumnLoadBatchLimit(crossColumnLimit)
                .singleQueryLoadBatchLimit(singleQueryLimit)
                .build());
    }

    @Test
    public void respondsToChangesInSingleQueryLimitBatchParameter() {
        AtomicReference<CassandraCellLoadingConfig> config = new AtomicReference<>(LOADING_CONFIG);
        CellLoadingBatcher reloadingBatcher = new CellLoadingBatcher(config::get, rebatchingCallback);
        updateConfig(config, CROSS_COLUMN_LIMIT, 2 * SINGLE_QUERY_LIMIT);

        List<List<Cell>> largerBatches =
                reloadingBatcher.partitionIntoBatches(rowRange(2 * SINGLE_QUERY_LIMIT, 0), ADDRESS, TABLE_REFERENCE);
        assertBatchContentsMatch(largerBatches, rowRange(2 * SINGLE_QUERY_LIMIT, 0));
        verify(rebatchingCallback, never()).consume(any(), any(), anyInt());
    }

    @Test
    public void fuzzTestPreservesBatcherInvariants() {
        Random random = new Random(SEED);
        List<Cell> randomCells = IntStream.range(0, 2_000)
                .mapToObj(unused -> cell(random.nextInt(100), random.nextInt(200)))
                .distinct()
                .collect(Collectors.toList());
        List<List<Cell>> batches = partitionUsingMockCallback(randomCells);

        assertThat(batches)
                .as("batch size should be limited by the single query limit")
                .allMatch(batch -> batch.size() <= SINGLE_QUERY_LIMIT);
        assertThat(batches)
                .as("mixed column batches should not exceed the cross column query limit")
                .allMatch(CellLoadingBatcherTest::batchIsSingleColumnOrSatisfiesCrossColumnLimit);
        assertThat(batches.stream().flatMap(Collection::stream))
                .as("output of batching should contain an arrangement of the same cells as in the input")
                .hasSameElementsAs(randomCells);
    }

    private List<List<Cell>> partitionUsingMockCallback(List<Cell> cells) {
        return batcher.partitionIntoBatches(cells, ADDRESS, TABLE_REFERENCE);
    }

    private static boolean batchIsSingleColumnOrSatisfiesCrossColumnLimit(List<Cell> batch) {
        return batchIsSingleColumn(batch) || batch.size() <= CROSS_COLUMN_LIMIT;
    }

    private static boolean batchIsSingleColumn(List<Cell> batch) {
        return batch.stream()
                        .map(Cell::getColumnName)
                        .collect(
                                Collectors.toCollection(() -> new TreeSet<>(UnsignedBytes.lexicographicalComparator())))
                        .size()
                == 1;
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
