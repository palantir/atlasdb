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

package com.palantir.atlasdb.transaction.impl.expectations;

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
import com.palantir.atlasdb.util.Measurable;
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

public final class ExpectationsMeasuringUtilsTest {
    private static final int SIZE_1 = 123;
    private static final int SIZE_2 = 214;
    private static final int SIZE_3 = 329;
    private static final Measurable MEASURABLE_1 = () -> SIZE_1;
    private static final Measurable MEASURABLE_2 = () -> SIZE_2;

    @Test
    public void sizeOfEmptyObjectsIsZero() {
        assertThat(ExpectationsMeasuringUtils.sizeInBytes(List.of())).isEqualTo(0);
        assertThat(ExpectationsMeasuringUtils.sizeInBytes(Set.of())).isEqualTo(0);
        assertThat(ExpectationsMeasuringUtils.sizeInBytes(Map.of())).isEqualTo(0);
        assertThat(ExpectationsMeasuringUtils.sizeInBytes(ImmutableMultimap.of()))
                .isEqualTo(0);
        assertThat(ExpectationsMeasuringUtils.toLongSizeInBytes(Map.of())).isEqualTo(0);
        assertThat(ExpectationsMeasuringUtils.toArraySizeInBytes(Map.of())).isEqualTo(0);
        assertThat(ExpectationsMeasuringUtils.byteArraysSizeInBytes(List.of())).isEqualTo(0);
        assertThat(ExpectationsMeasuringUtils.pageByRequestSizeInBytes(Map.of()))
                .isEqualTo(0);
    }

    @Test
    public void sizeOfMeasurableToLongMapIsCorrect() {
        Map<Measurable, Long> toLong = ImmutableMap.<Measurable, Long>builder()
                .put(MEASURABLE_1, 0L)
                .put(MEASURABLE_2, 1L)
                .buildOrThrow();

        assertThat(ExpectationsMeasuringUtils.toLongSizeInBytes(toLong)).isEqualTo(SIZE_1 + SIZE_2 + 2L * Long.BYTES);
    }

    @Test
    public void sizeOfMeasurableToByteArrayIsCorrect() {
        Map<Measurable, byte[]> toByteArray = ImmutableMap.<Measurable, byte[]>builder()
                .put(MEASURABLE_1, createBytes(SIZE_3))
                .put(MEASURABLE_2, createBytes(SIZE_3))
                .buildOrThrow();

        assertThat(ExpectationsMeasuringUtils.toArraySizeInBytes(toByteArray)).isEqualTo(SIZE_1 + SIZE_2 + 2L * SIZE_3);
    }

    @Test
    public void sizeOfByteArrayCollectionIsCorrect() {
        assertThat(ExpectationsMeasuringUtils.byteArraysSizeInBytes(List.of(PtBytes.EMPTY_BYTE_ARRAY)))
                .isEqualTo(0);
        assertThat(ExpectationsMeasuringUtils.byteArraysSizeInBytes(List.of(createBytes(SIZE_1))))
                .isEqualTo(SIZE_1);
        assertThat(ExpectationsMeasuringUtils.byteArraysSizeInBytes(
                        List.of(createBytes(SIZE_1), createBytes(SIZE_2), createBytes(SIZE_2))))
                .isEqualTo(SIZE_1 + 2L * SIZE_2);
    }

    @Test
    public void sizeOfRowResultOfSetOfLongIsCorrect() {
        assertThat(ExpectationsMeasuringUtils.setResultSizeInBytes(
                        RowResult.of(createCellWithByteSize(SIZE_1), Set.of())))
                .isEqualTo(SIZE_1);

        assertThat(ExpectationsMeasuringUtils.setResultSizeInBytes(
                        RowResult.of(createCellWithByteSize(SIZE_2), Set.of(1L))))
                .isEqualTo(Long.sum(SIZE_2, Long.BYTES));

        assertThat(ExpectationsMeasuringUtils.setResultSizeInBytes(
                        RowResult.of(createCellWithByteSize(SIZE_3), Set.of(1L, 2L, 3L))))
                .isEqualTo(SIZE_3 + 3L * Long.BYTES);
    }

    @Test
    public void sizeOfPageByRequestIsCorrect() {
        assertThat(ExpectationsMeasuringUtils.pageByRequestSizeInBytes(
                        ImmutableMap.<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>>builder()
                                .put(RangeRequest.all(), createPage(SIZE_1, 1, SIZE_1))
                                .put(
                                        RangeRequest.builder()
                                                .startRowInclusive(createBytes(SIZE_3))
                                                .build(),
                                        createPage(SIZE_2, 2, SIZE_3))
                                .put(
                                        RangeRequest.builder()
                                                .endRowExclusive(createBytes(SIZE_3))
                                                .build(),
                                        createPage(SIZE_1, 10, SIZE_2))
                                .buildOrThrow()))
                .isEqualTo(SIZE_1 + 2L * SIZE_3 + 10L * SIZE_2);
    }

    @Test
    public void sizeOfMeasurableToLongMultimapIsCorrect() {
        Multimap<Measurable, Long> toLong = ImmutableSetMultimap.<Measurable, Long>builder()
                .put(MEASURABLE_1, 0L)
                .put(MEASURABLE_2, 0L)
                .put(MEASURABLE_2, 1L)
                .build();

        assertThat(ExpectationsMeasuringUtils.sizeInBytes(toLong)).isEqualTo(SIZE_1 + 2L * SIZE_2 + 3L * Long.BYTES);
    }

    @Test
    public void sizeOfMeasurableToMeasurableEntryIsCorrect() {
        Entry<Measurable, Measurable> entry = new SimpleImmutableEntry<>(MEASURABLE_1, MEASURABLE_2);
        assertThat(ExpectationsMeasuringUtils.sizeInBytes(entry)).isEqualTo(Long.sum(SIZE_1, SIZE_2));
    }

    @Test
    public void sizeOfCollectionsOfMeasurableObjectsIsCorrect() {
        assertThat(ExpectationsMeasuringUtils.sizeInBytes(Set.of(MEASURABLE_1))).isEqualTo(SIZE_1);
        assertThat(ExpectationsMeasuringUtils.sizeInBytes(List.of(MEASURABLE_1)))
                .isEqualTo(SIZE_1);
        assertThat(ExpectationsMeasuringUtils.sizeInBytes(Set.of(MEASURABLE_1, MEASURABLE_2)))
                .isEqualTo(Long.sum(SIZE_1, SIZE_2));
        assertThat(ExpectationsMeasuringUtils.sizeInBytes(List.of(MEASURABLE_1, MEASURABLE_2, MEASURABLE_1)))
                .isEqualTo(2L * SIZE_1 + SIZE_2);
    }

    @Test
    public void sizeOfMeasurableToMeasurableMapIsCorrect() {
        Map<Measurable, Measurable> map = ImmutableMap.<Measurable, Measurable>builder()
                .put(MEASURABLE_1, MEASURABLE_2)
                .put(MEASURABLE_2, MEASURABLE_1)
                .buildOrThrow();

        assertThat(ExpectationsMeasuringUtils.sizeInBytes(map)).isEqualTo(2L * SIZE_1 + 2L * SIZE_2);
    }

    @Test
    public void sizeOfRowResultOfMeasurableIsCorrect() {
        RowResult<Measurable> rowResult = RowResult.create(
                createBytes(SIZE_3),
                ImmutableSortedMap.<byte[], Measurable>orderedBy(UnsignedBytes.lexicographicalComparator())
                        .put(createBytes(SIZE_1), MEASURABLE_2)
                        .put(createBytes(SIZE_2), MEASURABLE_1)
                        .buildOrThrow());

        assertThat(ExpectationsMeasuringUtils.sizeInBytes(rowResult)).isEqualTo(2L * SIZE_1 + 2L * SIZE_2 + SIZE_3);
    }

    private static SimpleTokenBackedResultsPage<RowResult<Value>, byte[]> createPage(
            int tokenSize, int chunksCount, int rowResultSize) {
        return SimpleTokenBackedResultsPage.create(
                createBytes(tokenSize),
                Stream.generate(() -> createRowResultWithByteSize(rowResultSize))
                        .limit(chunksCount)
                        .collect(Collectors.toUnmodifiableList()));
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
