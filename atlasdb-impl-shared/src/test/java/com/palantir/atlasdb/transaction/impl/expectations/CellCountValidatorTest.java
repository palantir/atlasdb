/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

import static com.palantir.logsafe.testing.Assertions.assertThatLoggableExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.cache.CacheValue;
import com.palantir.atlasdb.transaction.api.exceptions.MoreCellsPresentThanExpectedException;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import java.util.Map;
import one.util.streamex.EntryStream;
import org.junit.jupiter.api.Test;

public class CellCountValidatorTest {
    private static final Map<Cell, byte[]> TWO_PRESENT_ENTRIES_MAP = Map.of(
            Cell.create(PtBytes.toBytes("foo"), PtBytes.toBytes("someColumn")),
            PtBytes.toBytes("bar"),
            Cell.create(PtBytes.toBytes("baz"), PtBytes.toBytes("someColumn")),
            PtBytes.toBytes("qux"));
    private static final Map<Cell, CacheValue> TWO_PRESENT_ONE_EMPTY_ENTRY_MAP = Map.of(
            Cell.create(PtBytes.toBytes("foo"), PtBytes.toBytes("someColumn")),
            CacheValue.of(PtBytes.toBytes("bar")),
            Cell.create(PtBytes.toBytes("foo2"), PtBytes.toBytes("someColumn")),
            CacheValue.of(PtBytes.toBytes("bar2")),
            Cell.create(PtBytes.toBytes("baz"), PtBytes.toBytes("someColumn")),
            CacheValue.empty());

    @Test
    public void testDontThrowIfFetchedLessCellsThanExpected() {
        assertThatCode(() -> {
                    CellCountValidator.validateFetchedLessOrEqualToExpected(1, Map.of());
                    CellCountValidator.validateCacheAndGetNonEmptyValuesCount(1, Map.of());

                    CellCountValidator.validateFetchedLessOrEqualToExpected(2, cellMapOf("foo", "bar"));
                    CellCountValidator.validateCacheAndGetNonEmptyValuesCount(2, cacheCellMapOf("foo", "bar"));
                })
                .doesNotThrowAnyException();
    }

    @Test
    public void testDontThrowIfFetchedCellsEqualToExpected() {
        assertThatCode(() -> {
                    CellCountValidator.validateFetchedLessOrEqualToExpected(0, Map.of());
                    CellCountValidator.validateCacheAndGetNonEmptyValuesCount(0, Map.of());

                    CellCountValidator.validateFetchedLessOrEqualToExpected(1, cellMapOf("foo", "bar"));
                    CellCountValidator.validateCacheAndGetNonEmptyValuesCount(1, cacheCellMapOf("foo", "bar"));
                })
                .doesNotThrowAnyException();
    }

    @Test
    public void testThrowsIfFetchedMoreCellsThanExpected() {
        assertThatLoggableExceptionThrownBy(
                        () -> CellCountValidator.validateFetchedLessOrEqualToExpected(1, TWO_PRESENT_ENTRIES_MAP))
                .isInstanceOf(MoreCellsPresentThanExpectedException.class)
                .hasExactlyArgs(
                        SafeArg.of("expectedNumberOfCells", 1L),
                        SafeArg.of("numberOfCellsRetrieved", 2),
                        UnsafeArg.of("retrievedCells", TWO_PRESENT_ENTRIES_MAP));

        Map<Cell, CacheValue> cachedValues = toCached(TWO_PRESENT_ENTRIES_MAP);
        assertThatLoggableExceptionThrownBy(
                        () -> CellCountValidator.validateCacheAndGetNonEmptyValuesCount(1, cachedValues))
                .isInstanceOf(MoreCellsPresentThanExpectedException.class)
                .hasExactlyArgs(
                        SafeArg.of("expectedNumberOfCells", 1L),
                        SafeArg.of("numberOfCellsRetrieved", 2),
                        UnsafeArg.of("retrievedCells", TWO_PRESENT_ENTRIES_MAP));
    }

    @Test
    public void testThrowsIfExpectingNegativeValue() {
        assertThatLoggableExceptionThrownBy(
                        () -> CellCountValidator.validateFetchedLessOrEqualToExpected(-1, TWO_PRESENT_ENTRIES_MAP))
                .isInstanceOf(MoreCellsPresentThanExpectedException.class)
                .hasExactlyArgs(
                        SafeArg.of("expectedNumberOfCells", -1L),
                        SafeArg.of("numberOfCellsRetrieved", 2),
                        UnsafeArg.of("retrievedCells", TWO_PRESENT_ENTRIES_MAP));

        Map<Cell, CacheValue> cachedValues = toCached(TWO_PRESENT_ENTRIES_MAP);
        assertThatLoggableExceptionThrownBy(
                        () -> CellCountValidator.validateCacheAndGetNonEmptyValuesCount(-1, cachedValues))
                .isInstanceOf(MoreCellsPresentThanExpectedException.class)
                .hasExactlyArgs(
                        SafeArg.of("expectedNumberOfCells", -1L),
                        SafeArg.of("numberOfCellsRetrieved", 2),
                        UnsafeArg.of("retrievedCells", TWO_PRESENT_ENTRIES_MAP));
    }

    @Test
    public void testEmptyCacheValuesDoesNotCountAgainstPresentCells() {
        assertThatCode(() ->
                        CellCountValidator.validateCacheAndGetNonEmptyValuesCount(2, TWO_PRESENT_ONE_EMPTY_ENTRY_MAP))
                .doesNotThrowAnyException();
    }

    @Test
    public void testReturnsValueOfNonEmptyEntriesInCacheAfterValidation() {
        assertThat(CellCountValidator.validateCacheAndGetNonEmptyValuesCount(2, TWO_PRESENT_ONE_EMPTY_ENTRY_MAP))
                .isEqualTo(2L);
    }

    @Test
    public void testThrowsIfMorePresentCachedCellsThanExpected() {
        Map<Cell, CacheValue> cachedValues = toCached(TWO_PRESENT_ENTRIES_MAP);

        assertThatLoggableExceptionThrownBy(
                        () -> CellCountValidator.validateCacheAndGetNonEmptyValuesCount(1, cachedValues))
                .isInstanceOf(MoreCellsPresentThanExpectedException.class)
                .hasExactlyArgs(
                        SafeArg.of("expectedNumberOfCells", 1L),
                        SafeArg.of("numberOfCellsRetrieved", 2),
                        UnsafeArg.of("retrievedCells", TWO_PRESENT_ENTRIES_MAP));
    }

    private static Map<Cell, byte[]> cellMapOf(String key, String value) {
        return Map.of(Cell.create(PtBytes.toBytes(key), PtBytes.toBytes("someColumn")), PtBytes.toBytes(value));
    }

    private static Map<Cell, CacheValue> cacheCellMapOf(String key, String value) {
        return EntryStream.of(cellMapOf(key, value)).mapValues(CacheValue::of).toMap();
    }

    private static Map<Cell, CacheValue> toCached(Map<Cell, byte[]> presentValues) {
        return EntryStream.of(presentValues).mapValues(CacheValue::of).toMap();
    }
}
