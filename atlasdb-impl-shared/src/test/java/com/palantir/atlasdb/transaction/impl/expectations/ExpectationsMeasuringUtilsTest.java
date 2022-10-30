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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.util.paging.SimpleTokenBackedResultsPage;
import com.palantir.util.paging.TokenBackedBasicResultsPage;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

public class ExpectationsMeasuringUtilsTest {
    private static final byte BYTE = (byte) 0xa;
    private static final int SIZE_1 = 123;
    private static final int SIZE_2 = 214;
    private static final int SIZE_3 = 329;
    private static final int TIMESTAMP = 100;

    @Test
    public void emptyValueByCellMapSizeIsZero() {
        assertThat(ExpectationsMeasuringUtils.valueByCellSizeInBytes(Map.<Cell, Value>of()))
                .isEqualTo(0);
    }

    @Test
    public void emptyLongByCellMapSizeInBytesIsZero() {
        assertThat(ExpectationsMeasuringUtils.sizeInBytes(Map.<Cell, Long>of())).isEqualTo(0);
    }

    @Test
    public void emptyLongByCellMultimapSizeInBytesIsZero() {
        assertThat(ExpectationsMeasuringUtils.sizeInBytes(ArrayListMultimap.<Cell, Long>create()))
                .isEqualTo(0);
    }

    @Test
    public void emptyByteArrayByTableReferenceMapSizeInBytesIsZero() {
        assertThat(ExpectationsMeasuringUtils.arrayByRefSizeInBytes(Map.<TableReference, byte[]>of()))
                .isEqualTo(0);
    }

    @Test
    public void emptyTokenPageByRangeRequestMapSizeInBytesIsZero() {
        assertThat(ExpectationsMeasuringUtils.pageByRequestSizeInBytes(
                        Map.<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>>of()))
                .isEqualTo(0);
    }

    @Test
    public void testLongByCellMapSize() {
        Map<Cell, Long> longByCell = IntStream.range(1, SIZE_1 + 1)
                .boxed()
                .collect(Collectors.toUnmodifiableMap(ExpectationsMeasuringUtilsTest::createCell, Long::valueOf));

        // expected value follows from https://en.wikipedia.org/wiki/1_%2B_2_%2B_3_%2B_4_%2B_%E2%8B%AF
        assertThat(ExpectationsMeasuringUtils.sizeInBytes(longByCell))
                .isEqualTo(Long.sum(Long.BYTES, (long) SIZE_1 + 1) * SIZE_1);
    }

    @Test
    public void testValueByCellMapSize() {
        Map<Cell, Value> valueByCell = IntStream.range(1, SIZE_1 + 1)
                .boxed()
                .collect(Collectors.toUnmodifiableMap(
                        ExpectationsMeasuringUtilsTest::createCell, size -> createValue(2 * size)));

        assertThat(ExpectationsMeasuringUtils.valueByCellSizeInBytes(valueByCell))
                .isEqualTo((2L * SIZE_1 + Long.BYTES + 2) * SIZE_1);
    }

    @Test
    public void testValueByLongMultimapSize() {
        Multimap<Cell, Long> valueByLong = IntStream.range(0, SIZE_2 * SIZE_1)
                .boxed()
                .collect(ImmutableSetMultimap.toImmutableSetMultimap(
                        valueInt -> createCell(1 + (valueInt % SIZE_1)), Long::valueOf));

        assertThat(ExpectationsMeasuringUtils.sizeInBytes(valueByLong))
                .isEqualTo(SIZE_2 * SIZE_1 * (SIZE_1 + Long.BYTES + 1L));
    }

    @Test
    public void testArrayByTableRefSize() {
        Map<TableReference, byte[]> arrayByTableRef = IntStream.range(1, 2 * SIZE_1 + 1)
                .boxed()
                .collect(Collectors.toUnmodifiableMap(
                        ExpectationsMeasuringUtilsTest::createTableReference,
                        ExpectationsMeasuringUtilsTest::spawnBytes));

        assertThat(ExpectationsMeasuringUtils.arrayByRefSizeInBytes(arrayByTableRef))
                .isEqualTo((Character.BYTES + 1L) * (2L * SIZE_1 + 1L) * SIZE_1);
    }

    @Test
    public void testValueRowResult() {
        assertThat(ExpectationsMeasuringUtils.sizeInBytes(RowResult.of(createCell(SIZE_1), createValue(SIZE_1))))
                .isEqualTo(3L * SIZE_1 + Long.BYTES);

        assertThat(ExpectationsMeasuringUtils.sizeInBytes(RowResult.of(createCell(SIZE_2), createValue(SIZE_2))))
                .isEqualTo(3L * SIZE_2 + Long.BYTES);
    }

    @Test
    public void testLongSetRowResultSize() {
        assertThat(ExpectationsMeasuringUtils.setResultSizeInBytes(RowResult.of(createCell(SIZE_1), Set.of())))
                .isEqualTo(2L * SIZE_1);

        assertThat(ExpectationsMeasuringUtils.setResultSizeInBytes(RowResult.of(createCell(SIZE_1), Set.of(1L))))
                .isEqualTo(2L * SIZE_1 + Long.BYTES);

        assertThat(ExpectationsMeasuringUtils.setResultSizeInBytes(RowResult.of(createCell(SIZE_1), Set.of(1L, 2L))))
                .isEqualTo(2L * SIZE_1 + 2L * Long.BYTES);

        assertThat(ExpectationsMeasuringUtils.setResultSizeInBytes(RowResult.of(
                        createCell(SIZE_1), LongStream.range(0, SIZE_1).boxed().collect(Collectors.toSet()))))
                .isEqualTo(SIZE_1 * (Long.BYTES + 2L));
    }

    @Test
    public void testValueByCellEntrySize() {
        assertThat(ExpectationsMeasuringUtils.sizeInBytes(
                        new SimpleImmutableEntry<>(createCell(SIZE_1), createValue(SIZE_1))))
                .isEqualTo(3L * SIZE_1 + Long.BYTES);

        assertThat(ExpectationsMeasuringUtils.sizeInBytes(
                        new SimpleImmutableEntry<>(createCell(SIZE_2), createValue(SIZE_2))))
                .isEqualTo(3L * SIZE_2 + Long.BYTES);
    }

    @Test
    public void testPageByRequestSize() {
        assertThat(ExpectationsMeasuringUtils.pageByRequestSizeInBytes(
                        ImmutableMap.<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>>builder()
                                .put(RangeRequest.all(), createPage(SIZE_2, SIZE_2, SIZE_2, 0))
                                .put(
                                        RangeRequest.builder()
                                                .startRowInclusive(spawnBytes(SIZE_3))
                                                .build(),
                                        createPage(SIZE_2, SIZE_2, SIZE_2, 1))
                                .put(
                                        RangeRequest.builder()
                                                .endRowExclusive(spawnBytes(SIZE_3))
                                                .build(),
                                        createPage(SIZE_3, SIZE_2, SIZE_2, SIZE_3))
                                .put(
                                        RangeRequest.builder()
                                                .startRowInclusive(spawnBytes(SIZE_2))
                                                .build(),
                                        createPage(SIZE_3, SIZE_3, SIZE_3, SIZE_3))
                                .buildOrThrow()))
                .isEqualTo((SIZE_3 + 1L) * (3L * SIZE_2 + Long.BYTES) + SIZE_3 * (3L * SIZE_3 + Long.BYTES));
    }

    private static SimpleTokenBackedResultsPage<RowResult<Value>, byte[]> createPage(
            int tokenSize, int cellNameSize, int valueNameSize, int resultsCount) {
        return SimpleTokenBackedResultsPage.create(
                spawnBytes(tokenSize),
                Stream.generate(() -> RowResult.of(createCell(cellNameSize), createValue(valueNameSize)))
                        .limit(resultsCount)
                        .collect(Collectors.toUnmodifiableList()));
    }

    private static TableReference createTableReference(int size) {
        return TableReference.createWithEmptyNamespace(StringUtils.repeat('A', size));
    }

    private static Cell createCell(int nameSize) {
        return Cell.create(spawnBytes(nameSize), spawnBytes(nameSize));
    }

    private static Value createValue(int size) {
        return Value.create(spawnBytes(size), TIMESTAMP);
    }

    private static byte[] spawnBytes(int size) {
        byte[] bytes = new byte[size];
        Arrays.fill(bytes, BYTE);
        return bytes;
    }
}
