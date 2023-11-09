/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.keyvalue.dbkvs.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class RowsColumnRangeBatchRequestsTest {

    public static List<Arguments> getParameters() {
        return List.of(
                Arguments.of(false, false),
                Arguments.of(false, true),
                Arguments.of(true, false),
                Arguments.of(true, true));
    }

    @ParameterizedTest(name = "Partial first row: {0}, partial last row: {1}")
    @MethodSource("getParameters")
    public void testPartitionSimple(boolean hasPartialFirstRow, boolean hasPartialLastRow) {
        testPartition(createRequest(1000, hasPartialFirstRow, hasPartialLastRow), 100);
    }

    @ParameterizedTest(name = "Partial first row: {0}, partial last row: {1}")
    @MethodSource("getParameters")
    public void testSinglePartition(boolean hasPartialFirstRow, boolean hasPartialLastRow) {
        testPartition(createRequest(10, hasPartialFirstRow, hasPartialLastRow), 100);
    }

    @ParameterizedTest(name = "Partial first row: {0}, partial last row: {1}")
    @MethodSource("getParameters")
    public void testPartitionSizeDoesNotDivideNumberOfRows(boolean hasPartialFirstRow, boolean hasPartialLastRow) {
        testPartition(createRequest(100, hasPartialFirstRow, hasPartialLastRow), 23);
    }

    private RowsColumnRangeBatchRequest createRequest(
            int numTotalRows, boolean hasPartialFirstRow, boolean hasPartialLastRow) {
        ColumnRangeSelection fullColumnRange = new ColumnRangeSelection(col(0), col(5));
        ImmutableRowsColumnRangeBatchRequest.Builder request =
                ImmutableRowsColumnRangeBatchRequest.builder().columnRangeSelection(fullColumnRange);
        if (hasPartialFirstRow) {
            request.partialFirstRow(Maps.immutableEntry(row(0), BatchColumnRangeSelection.create(col(3), col(5), 10)));
        }
        int firstFullRowIndex = hasPartialFirstRow ? 2 : 1;
        int lastFullRowIndex = hasPartialLastRow ? numTotalRows - 1 : numTotalRows;
        for (int rowNum = firstFullRowIndex; rowNum <= lastFullRowIndex; rowNum++) {
            request.addRowsToLoadFully(row(rowNum));
        }
        if (hasPartialLastRow) {
            request.partialLastRow(
                    Maps.immutableEntry(row(numTotalRows), BatchColumnRangeSelection.create(fullColumnRange, 10)));
        }
        return request.build();
    }

    private static byte[] row(int rowNum) {
        return Ints.toByteArray(rowNum);
    }

    private static byte[] col(int colNum) {
        return Ints.toByteArray(colNum);
    }

    private static void testPartition(RowsColumnRangeBatchRequest request, int partitionSize) {
        List<RowsColumnRangeBatchRequest> partitions = RowsColumnRangeBatchRequests.partition(request, partitionSize);
        assertIntermediatePartitionsHaveNoPartialRows(partitions);
        assertRowsInPartitionsMatchOriginal(request, partitions);
        assertColumnRangesInPartitionsMatchOriginal(request, partitions);
        assertPartitionsHaveCorrectSize(partitions, partitionSize);
    }

    private static void assertIntermediatePartitionsHaveNoPartialRows(List<RowsColumnRangeBatchRequest> partitions) {
        // No partition other than the first should have a partial first row
        for (RowsColumnRangeBatchRequest partition : partitions.subList(1, partitions.size())) {
            assertThat(partition.getPartialFirstRow()).isNotPresent();
        }
        // No partition other than the last should have a partial last row
        for (RowsColumnRangeBatchRequest partition : partitions.subList(0, partitions.size() - 1)) {
            assertThat(partition.getPartialLastRow()).isNotPresent();
        }
    }

    private static void assertRowsInPartitionsMatchOriginal(
            RowsColumnRangeBatchRequest original, List<RowsColumnRangeBatchRequest> partitions) {
        List<byte[]> actualAllRows = partitions.stream()
                .flatMap(partition -> RowsColumnRangeBatchRequests.getAllRowsInOrder(partition).stream())
                .collect(Collectors.toList());
        assertThat(actualAllRows).containsExactlyElementsOf(RowsColumnRangeBatchRequests.getAllRowsInOrder(original));
    }

    private static void assertColumnRangesInPartitionsMatchOriginal(
            RowsColumnRangeBatchRequest request, List<RowsColumnRangeBatchRequest> partitions) {
        assertThat(partitions.get(0).getPartialFirstRow()).isEqualTo(request.getPartialFirstRow());
        assertThat(partitions.get(partitions.size() - 1).getPartialLastRow()).isEqualTo(request.getPartialLastRow());

        for (RowsColumnRangeBatchRequest partition : partitions) {
            assertThat(partition.getRowsToLoadFully().isEmpty()
                            || partition.getColumnRangeSelection().equals(request.getColumnRangeSelection()))
                    .isTrue();
        }
    }

    private static void assertPartitionsHaveCorrectSize(
            List<RowsColumnRangeBatchRequest> partitions, int expectedSize) {
        for (int i = 0; i < partitions.size(); i++) {
            int actualPartitionSize = RowsColumnRangeBatchRequests.getAllRowsInOrder(partitions.get(i))
                    .size();
            if (i < partitions.size() - 1) {
                assertThat(actualPartitionSize).isEqualTo(expectedSize);
            } else {
                assertThat(actualPartitionSize).isLessThanOrEqualTo(expectedSize);
            }
        }
    }
}
