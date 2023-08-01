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

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.cache.CacheValue;
import com.palantir.atlasdb.transaction.api.exceptions.MoreCellsPresentThanExpectedException;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import one.util.streamex.EntryStream;
import org.junit.Test;

public class CellCountValidatorTest {

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
        Map<Cell, byte[]> presentValues = cellMapOf("foo", "bar", "baz", "qux");
        assertThatLoggableExceptionThrownBy(
                        () -> CellCountValidator.validateFetchedLessOrEqualToExpected(1, presentValues))
                .isInstanceOf(MoreCellsPresentThanExpectedException.class)
                .hasExactlyArgs(
                        SafeArg.of("expectedNumberOfCells", 1L),
                        SafeArg.of("numberOfCellsRetrieved", 2),
                        UnsafeArg.of("retrievedCells", presentValues));

        Map<Cell, CacheValue> cachedValues = toCached(presentValues);
        assertThatLoggableExceptionThrownBy(
                        () -> CellCountValidator.validateCacheAndGetNonEmptyValuesCount(1, cachedValues))
                .isInstanceOf(MoreCellsPresentThanExpectedException.class)
                .hasExactlyArgs(
                        SafeArg.of("expectedNumberOfCells", 1L),
                        SafeArg.of("numberOfCellsRetrieved", 2),
                        UnsafeArg.of("retrievedCells", presentValues));
    }

    @Test
    public void testThrowsIfExpectingNegativeValue() {
        Map<Cell, byte[]> presentValues = cellMapOf("foo", "bar", "baz", "qux");
        assertThatLoggableExceptionThrownBy(
                        () -> CellCountValidator.validateFetchedLessOrEqualToExpected(-1, presentValues))
                .isInstanceOf(MoreCellsPresentThanExpectedException.class)
                .hasExactlyArgs(
                        SafeArg.of("expectedNumberOfCells", -1L),
                        SafeArg.of("numberOfCellsRetrieved", 2),
                        UnsafeArg.of("retrievedCells", presentValues));

        Map<Cell, CacheValue> cachedValues = toCached(presentValues);
        assertThatLoggableExceptionThrownBy(
                        () -> CellCountValidator.validateCacheAndGetNonEmptyValuesCount(-1, cachedValues))
                .isInstanceOf(MoreCellsPresentThanExpectedException.class)
                .hasExactlyArgs(
                        SafeArg.of("expectedNumberOfCells", -1L),
                        SafeArg.of("numberOfCellsRetrieved", 2),
                        UnsafeArg.of("retrievedCells", presentValues));
    }

    @Test
    public void testEmptyCacheValuesDoesNotCountAgainstPresentCells() {
        Map<Cell, CacheValue> cachedValues = Map.of(
                Cell.create("foo".getBytes(StandardCharsets.UTF_8), "someColumn".getBytes(StandardCharsets.UTF_8)),
                CacheValue.of("bar".getBytes(StandardCharsets.UTF_8)),
                Cell.create("baz".getBytes(StandardCharsets.UTF_8), "someColumn".getBytes(StandardCharsets.UTF_8)),
                CacheValue.empty());

        assertThatCode(() -> CellCountValidator.validateCacheAndGetNonEmptyValuesCount(1, cachedValues))
                .doesNotThrowAnyException();
    }

    @Test
    public void testReturnsValueOfNonEmptyEntriesInCacheAfterValidation() {
        Map<Cell, CacheValue> cachedValues = Map.of(
                Cell.create("foo".getBytes(StandardCharsets.UTF_8), "someColumn".getBytes(StandardCharsets.UTF_8)),
                CacheValue.of("bar".getBytes(StandardCharsets.UTF_8)),
                Cell.create("foo2".getBytes(StandardCharsets.UTF_8), "someColumn".getBytes(StandardCharsets.UTF_8)),
                CacheValue.of("bar2".getBytes(StandardCharsets.UTF_8)),
                Cell.create("baz".getBytes(StandardCharsets.UTF_8), "someColumn".getBytes(StandardCharsets.UTF_8)),
                CacheValue.empty());

        assertThat(CellCountValidator.validateCacheAndGetNonEmptyValuesCount(2, cachedValues))
                .isEqualTo(2L);
    }

    @Test
    public void testThrowsIfMorePresentCachedCellsThanExpected() {
        Map<Cell, byte[]> rawValues = cellMapOf("foo", "bar", "baz", "qux");
        Map<Cell, CacheValue> cachedValues = toCached(rawValues);

        assertThatLoggableExceptionThrownBy(
                        () -> CellCountValidator.validateCacheAndGetNonEmptyValuesCount(1, cachedValues))
                .isInstanceOf(MoreCellsPresentThanExpectedException.class)
                .hasExactlyArgs(
                        SafeArg.of("expectedNumberOfCells", 1L),
                        SafeArg.of("numberOfCellsRetrieved", 2),
                        UnsafeArg.of("retrievedCells", rawValues));
    }

    private Map<Cell, byte[]> cellMapOf(String key, String value, String key2, String value2) {
        return Map.of(
                Cell.create(key.getBytes(StandardCharsets.UTF_8), "someColumn".getBytes(StandardCharsets.UTF_8)),
                value.getBytes(StandardCharsets.UTF_8),
                Cell.create(key2.getBytes(StandardCharsets.UTF_8), "someColumn".getBytes(StandardCharsets.UTF_8)),
                value2.getBytes(StandardCharsets.UTF_8));
    }

    private Map<Cell, byte[]> cellMapOf(String key, String value) {
        return Map.of(
                Cell.create(key.getBytes(StandardCharsets.UTF_8), "someColumn".getBytes(StandardCharsets.UTF_8)),
                value.getBytes(StandardCharsets.UTF_8));
    }

    private Map<Cell, CacheValue> cacheCellMapOf(String key, String value) {
        return EntryStream.of(cellMapOf(key, value)).mapValues(CacheValue::of).toMap();
    }

    private Map<Cell, CacheValue> toCached(Map<Cell, byte[]> presentValues) {
        return EntryStream.of(presentValues).mapValues(CacheValue::of).toMap();
    }
}