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

package com.palantir.atlasdb.util;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Multimap;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.logsafe.Preconditions;
import com.palantir.util.paging.SimpleTokenBackedResultsPage;
import com.palantir.util.paging.TokenBackedBasicResultsPage;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Test;

public final class MeasuringUtilsTest {
    private static final int SIZE_1 = 123;
    private static final int SIZE_2 = 228;
    private static final int SIZE_3 = 599;
    private static final Measurable MEASURABLE_1 = () -> SIZE_1;
    private static final Measurable MEASURABLE_2 = () -> SIZE_2;

    @Test
    public void sizeOfEmptyObjectsIsZero() {
        assertThat(MeasuringUtils.sizeOf(List.of())).isEqualTo(0);
        assertThat(MeasuringUtils.sizeOf(Set.of())).isEqualTo(0);
        assertThat(MeasuringUtils.sizeOf(Map.of())).isEqualTo(0);
        assertThat(MeasuringUtils.sizeOf(ImmutableMultimap.of())).isEqualTo(0);
        assertThat(MeasuringUtils.sizeOfMeasurableLongMap(Map.of())).isEqualTo(0);
        assertThat(MeasuringUtils.sizeOfMeasurableByteMap(Map.of())).isEqualTo(0);
        assertThat(MeasuringUtils.sizeOfByteCollection(List.of())).isEqualTo(0);
        assertThat(MeasuringUtils.sizeOfPageByRangeRequestMap(Map.of())).isEqualTo(0);
    }

    @Test
    public void sizeOfMeasurableToLongMapIsCorrect() {
        Map<Measurable, Long> toLong = ImmutableMap.of(
                MEASURABLE_1, 0L,
                MEASURABLE_2, 1L);

        assertThat(MeasuringUtils.sizeOfMeasurableLongMap(toLong)).isEqualTo(SIZE_1 + SIZE_2 + 2L * Long.BYTES);
    }

    @Test
    public void sizeOfMeasurableToByteArrayMapIsCorrect() {
        Map<Measurable, byte[]> toByteArray = ImmutableMap.of(
                MEASURABLE_1, createBytes(SIZE_3),
                MEASURABLE_2, createBytes(SIZE_3));

        assertThat(MeasuringUtils.sizeOfMeasurableByteMap(toByteArray)).isEqualTo(SIZE_1 + SIZE_2 + 2L * SIZE_3);
    }

    @Test
    public void sizeOfByteArrayCollectionIsCorrect() {
        assertThat(MeasuringUtils.sizeOfByteCollection(List.of(PtBytes.EMPTY_BYTE_ARRAY)))
                .isEqualTo(0);
        assertThat(MeasuringUtils.sizeOfByteCollection(List.of(createBytes(SIZE_1))))
                .isEqualTo(SIZE_1);
        assertThat(MeasuringUtils.sizeOfByteCollection(
                        List.of(createBytes(SIZE_1), createBytes(SIZE_2), createBytes(SIZE_2))))
                .isEqualTo(SIZE_1 + 2L * SIZE_2);
    }

    @Test
    public void sizeOfRowResultOfSetOfLongIsCorrect() {
        assertThat(MeasuringUtils.sizeOfLongSetRowResult(RowResult.of(createCellWithByteSize(SIZE_1), Set.of())))
                .isEqualTo(SIZE_1);

        assertThat(MeasuringUtils.sizeOfLongSetRowResult(RowResult.of(createCellWithByteSize(SIZE_2), Set.of(1L))))
                .isEqualTo(Long.sum(SIZE_2, Long.BYTES));

        assertThat(MeasuringUtils.sizeOfLongSetRowResult(
                        RowResult.of(createCellWithByteSize(SIZE_3), Set.of(1L, 2L, 3L))))
                .isEqualTo(SIZE_3 + 3L * Long.BYTES);
    }

    @Test
    public void pageByRequestMapWithEmptyPageHasSizeZero() {
        assertThat(MeasuringUtils.sizeOfPageByRangeRequestMap(
                        ImmutableMap.of(createRangeRequest(1, 0), createPage(0, 0, 0))))
                .isEqualTo(0);
    }

    @Test
    public void pageByRequestMapSizeIncludesValueSizes() {
        assertThat(MeasuringUtils.sizeOfPageByRangeRequestMap(ImmutableMap.of(
                        createRangeRequest(1, SIZE_2), createPage(SIZE_1, 2, SIZE_1),
                        createRangeRequest(2, SIZE_2), createPage(SIZE_1, 8, SIZE_1))))
                .isGreaterThanOrEqualTo(10L * SIZE_1);
    }

    @Test
    public void pageByRequestMapSizeIgnoresRangeRequestSizes() {
        Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> firstMap = ImmutableMap.of(
                createRangeRequest(1, SIZE_2), createPage(SIZE_1, 2, SIZE_1),
                createRangeRequest(2, SIZE_2), createPage(SIZE_1, 5, SIZE_1));

        Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> secondMap = ImmutableMap.of(
                createRangeRequest(1, SIZE_3), createPage(SIZE_1, 2, SIZE_1),
                createRangeRequest(2, SIZE_3), createPage(SIZE_1, 5, SIZE_1));

        assertThat(MeasuringUtils.sizeOfPageByRangeRequestMap(firstMap))
                .isEqualTo(MeasuringUtils.sizeOfPageByRangeRequestMap(secondMap));
    }

    @Test
    public void pageByRequestMapSizeIgnoresTokenSizes() {
        Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> firstMap = ImmutableMap.of(
                createRangeRequest(1, SIZE_1), createPage(SIZE_2, 2, SIZE_1),
                createRangeRequest(2, SIZE_1), createPage(SIZE_2, 5, SIZE_1));

        Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> secondMap = ImmutableMap.of(
                createRangeRequest(1, SIZE_1), createPage(SIZE_3, 2, SIZE_1),
                createRangeRequest(2, SIZE_1), createPage(SIZE_3, 5, SIZE_1));

        assertThat(MeasuringUtils.sizeOfPageByRangeRequestMap(firstMap))
                .isEqualTo(MeasuringUtils.sizeOfPageByRangeRequestMap(secondMap));
    }

    @Test
    public void sizeOfPageByRequestIsCorrect() {
        assertThat(MeasuringUtils.sizeOfPageByRangeRequestMap(ImmutableMap.of(
                        createRangeRequest(1, SIZE_1), createPage(SIZE_1, 1, SIZE_1),
                        createRangeRequest(2, SIZE_2), createPage(SIZE_2, 2, SIZE_3),
                        createRangeRequest(3, SIZE_3), createPage(SIZE_1, 10, SIZE_2))))
                .isEqualTo(SIZE_1 + 2L * SIZE_3 + 10L * SIZE_2);
    }

    @Test
    public void sizeOfMeasurableToLongMultimapIsCorrect() {
        Multimap<Measurable, Long> toLong = ImmutableSetMultimap.of(
                MEASURABLE_1, 0L,
                MEASURABLE_2, 0L,
                MEASURABLE_2, 1L);

        assertThat(MeasuringUtils.sizeOf(toLong)).isEqualTo(SIZE_1 + 2L * SIZE_2 + 3L * Long.BYTES);
    }

    @Test
    public void sizeOfMeasurableToMeasurableEntryIsCorrect() {
        Entry<Measurable, Measurable> entry = new SimpleImmutableEntry<>(MEASURABLE_1, MEASURABLE_2);
        assertThat(MeasuringUtils.sizeOf(entry)).isEqualTo(Long.sum(SIZE_1, SIZE_2));
    }

    @Test
    public void sizeOfCollectionsOfMeasurableObjectsIsCorrect() {
        assertThat(MeasuringUtils.sizeOf(Set.of(MEASURABLE_1))).isEqualTo(SIZE_1);
        assertThat(MeasuringUtils.sizeOf(List.of(MEASURABLE_1))).isEqualTo(SIZE_1);
        assertThat(MeasuringUtils.sizeOf(Set.of(MEASURABLE_1, MEASURABLE_2))).isEqualTo(Long.sum(SIZE_1, SIZE_2));
        assertThat(MeasuringUtils.sizeOf(List.of(MEASURABLE_1, MEASURABLE_2, MEASURABLE_1)))
                .isEqualTo(2L * SIZE_1 + SIZE_2);
    }

    @Test
    public void sizeOfMeasurableToMeasurableMapIsCorrect() {
        Map<Measurable, Measurable> map = ImmutableMap.of(
                MEASURABLE_1, MEASURABLE_2,
                MEASURABLE_2, MEASURABLE_1);

        assertThat(MeasuringUtils.sizeOf(map)).isEqualTo(2L * SIZE_1 + 2L * SIZE_2);
    }

    @Test
    public void sizeOfRowResultOfMeasurableIsCorrect() {
        RowResult<Measurable> rowResult = RowResult.create(
                createBytes(SIZE_3),
                ImmutableSortedMap.<byte[], Measurable>orderedBy(UnsignedBytes.lexicographicalComparator())
                        .put(createBytes(SIZE_1), MEASURABLE_2)
                        .put(createBytes(SIZE_2), MEASURABLE_1)
                        .buildOrThrow());

        assertThat(MeasuringUtils.sizeOf(rowResult)).isEqualTo(2L * SIZE_1 + 2L * SIZE_2 + SIZE_3);
    }

    private static SimpleTokenBackedResultsPage<RowResult<Value>, byte[]> createPage(
            int tokenSize, int chunksCount, int rowResultSize) {
        return SimpleTokenBackedResultsPage.create(
                createBytes(tokenSize),
                Stream.generate(() -> createRowResultWithByteSize(rowResultSize))
                        .limit(chunksCount)
                        .collect(Collectors.toUnmodifiableList()));
    }

    private static RangeRequest createRangeRequest(int identifier, int rowSize) {
        return RangeRequest.builder()
                .startRowInclusive(createBytes(rowSize))
                .batchHint(identifier)
                .build();
    }

    private static RowResult<Value> createRowResultWithByteSize(int size) {
        Preconditions.checkArgument(
                size >= 2 * Long.BYTES, "Size should be at least twice the size in bytes of a long value");
        return RowResult.of(createCellWithByteSize(size / 2), createValueWithByteSize(size - (size / 2)));
    }

    private static Cell createCellWithByteSize(int size) {
        Preconditions.checkArgument(size >= 2, "Size should be at least two");
        return Cell.create(createBytes(size / 2), createBytes(size - (size / 2)));
    }

    private static Value createValueWithByteSize(int size) {
        Preconditions.checkArgument(size >= Long.BYTES, "Size should be at least the size in bytes of a long value");
        return Value.create(createBytes(size - Long.BYTES), Value.INVALID_VALUE_TIMESTAMP);
    }

    private static byte[] createBytes(int size) {
        return new byte[size];
    }
}
