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
package com.palantir.atlasdb.cleaner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Streams;
import com.google.common.math.LongMath;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.common.streams.KeyedStream;
import java.math.RoundingMode;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class KeyValueServicePuncherStoreTest {
    private static final long TIMESTAMP_1 = 10L;
    private static final long TIMESTAMP_2 = 20L;
    private static final long TIMESTAMP_3 = 30L;
    private static final long TIMESTAMP_BETWEEN_1_AND_2 = mean(TIMESTAMP_1, TIMESTAMP_2);

    private static final long WALL_CLOCK_1 = 100L;
    private static final long WALL_CLOCK_2 = 200L;
    private static final long WALL_CLOCK_3 = 300L;
    private static final long WALL_CLOCK_BETWEEN_1_AND_2 = mean(WALL_CLOCK_1, WALL_CLOCK_2);

    private static final ImmutableMap<Long, Long> PUNCHER_HISTORY = ImmutableMap.of(
            TIMESTAMP_1, WALL_CLOCK_1,
            TIMESTAMP_2, WALL_CLOCK_2,
            TIMESTAMP_3, WALL_CLOCK_3);
    private static final ImmutableMap<Long, Long> PUNCHER_HISTORY_WITH_CLOCK_DRIFT = ImmutableMap.of(
            TIMESTAMP_1, WALL_CLOCK_2,
            TIMESTAMP_2, WALL_CLOCK_1);
    private static final long MAX_OFFSET_FOR_BOUNDS_TESTS =
            KeyValueServicePuncherStore.MAX_RANGE_SCAN_SIZE * LongMath.pow(2, 10);

    private PuncherStore puncherStore;

    @Before
    public void setUp() {
        puncherStore = initializePuncherStore(PUNCHER_HISTORY);
    }

    @Test
    public void getTimestampForMillisReturnsMinValueIfQueryingClockTimeBeforeFirstPunch() {
        assertThat(puncherStore.get(WALL_CLOCK_1 - 1)).isEqualTo(Long.MIN_VALUE);
        assertThat(puncherStore.get(0L)).isEqualTo(Long.MIN_VALUE);
    }

    @Test
    public void getTimestampForMillisReturnsTimestampIfQueryingPreciseClockTime() {
        PUNCHER_HISTORY.forEach(
                (key, value) -> assertThat(puncherStore.get(value)).isEqualTo(key));
    }

    @Test
    public void getTimestampForMillisReturnsLastTimestampKnownToOccurBeforeQueriedTime() {
        PUNCHER_HISTORY.forEach(
                (key, value) -> assertThat(puncherStore.get(value + 1)).isEqualTo(key));
    }

    @Test
    public void getMillisForTimestampReturnsZeroIfQueryingTimestampBeforeFirstPunch() {
        assertThat(puncherStore.getMillisForTimestamp(Long.MIN_VALUE)).isEqualTo(0L);
        assertThat(puncherStore.getMillisForTimestamp(TIMESTAMP_1 - 1)).isEqualTo(0L);
    }

    @Test
    public void getMillisForTimestampReturnsClockTimeIfQueryingPreciseTimestamp() {
        PUNCHER_HISTORY.forEach((key, value) ->
                assertThat(puncherStore.getMillisForTimestamp(key)).isEqualTo(value));
    }

    @Test
    public void getMillisForTimestampReturnsLastTimeKnownToOccurBeforeQueriedTimestamp() {
        PUNCHER_HISTORY.forEach((key, value) ->
                assertThat(puncherStore.getMillisForTimestamp(key + 1)).isEqualTo(value));
    }

    @Test
    public void handlesPunchesNotInSequentialOrder() {
        assertThat(puncherStore.getMillisForTimestamp(TIMESTAMP_BETWEEN_1_AND_2))
                .isEqualTo(WALL_CLOCK_1);
        puncherStore.put(TIMESTAMP_BETWEEN_1_AND_2, WALL_CLOCK_BETWEEN_1_AND_2);
        assertThat(puncherStore.getMillisForTimestamp(TIMESTAMP_BETWEEN_1_AND_2))
                .isEqualTo(WALL_CLOCK_BETWEEN_1_AND_2);
    }

    @Test
    public void returnsGreatestPunchedTimeBeforeTimestampEvenIfNotAssociatedWithGreatestEligibleTimestamp() {
        puncherStore = initializePuncherStore(PUNCHER_HISTORY_WITH_CLOCK_DRIFT);
        assertThat(puncherStore.getMillisForTimestamp(TIMESTAMP_2))
                .isEqualTo(WALL_CLOCK_2)
                .isNotEqualTo(WALL_CLOCK_1); // strictly speaking not needed but better for readability
    }

    @Test
    public void returnsTimestampAssociatedWithGreatestPunchedTimeEvenIfItIsNotGreatest() {
        puncherStore = initializePuncherStore(PUNCHER_HISTORY_WITH_CLOCK_DRIFT);
        assertThat(puncherStore.get(WALL_CLOCK_2))
                .isEqualTo(TIMESTAMP_1)
                .isNotEqualTo(TIMESTAMP_2); // strictly speaking not needed but better for readability
    }

    @Test
    public void getOlderStrictlyBefore() {
        KeyValueService kvs = Mockito.spy(new InMemoryKeyValueService(false));
        // Punched: (100, 10), (200, 20), (300, 30)
        puncherStore = initializePuncherStore(PUNCHER_HISTORY, kvs);
        assertGetOlderReturnsMillisAndTimestamp(kvs, 400, 300, 30);
        assertGetOlderReturnsMillisAndTimestamp(kvs, 250, 200, 20);
        assertGetOlderReturnsMillisAndTimestamp(kvs, 150, 100, 10);

        verify(kvs, times(3)).getRange(eq(AtlasDbConstants.PUNCH_TABLE), any(RangeRequest.class), eq(Long.MAX_VALUE));
    }

    @Test
    public void getOlderExactMatch() {
        KeyValueService kvs = Mockito.spy(new InMemoryKeyValueService(false));
        // Punched: (100, 10), (200, 20), (300, 30)
        puncherStore = initializePuncherStore(PUNCHER_HISTORY, kvs);
        assertGetOlderReturnsMillisAndTimestamp(kvs, 300, 300, 30);
        assertGetOlderReturnsMillisAndTimestamp(kvs, 200, 200, 20);
        assertGetOlderReturnsMillisAndTimestamp(kvs, 100, 100, 10);

        verify(kvs, times(3)).getRange(eq(AtlasDbConstants.PUNCH_TABLE), any(RangeRequest.class), eq(Long.MAX_VALUE));
    }

    @Test
    public void getOlderNoEntry() {
        KeyValueService kvs = Mockito.spy(new InMemoryKeyValueService(false));
        // Punched: (100, 10), (200, 20), (300, 30)
        puncherStore = initializePuncherStore(PUNCHER_HISTORY, kvs);
        assertGetOlderReturnsMillisAndNoTimestamp(kvs, 50, 50);
        assertGetOlderReturnsMillisAndNoTimestamp(kvs, 10, 10);

        verify(kvs, times(2)).getRange(eq(AtlasDbConstants.PUNCH_TABLE), any(RangeRequest.class), eq(Long.MAX_VALUE));
    }

    @Test
    public void getOlderWithClockSkew() {
        KeyValueService kvs = Mockito.spy(new InMemoryKeyValueService(false));
        // Punched: (100, 20), (200, 10)
        puncherStore = initializePuncherStore(PUNCHER_HISTORY_WITH_CLOCK_DRIFT, kvs);
        assertGetOlderReturnsMillisAndTimestamp(kvs, 400, 200, 10);
        assertGetOlderReturnsMillisAndTimestamp(kvs, 150, 100, 20);

        verify(kvs, times(2)).getRange(eq(AtlasDbConstants.PUNCH_TABLE), any(RangeRequest.class), eq(Long.MAX_VALUE));
    }

    @Test
    public void findOlderReturnsReasonableEstimate() {
        KeyValueService kvs = Mockito.spy(new InMemoryKeyValueService(false));
        PuncherStore puncherStore = KeyValueServicePuncherStore.create(kvs);

        int maxReads = 10;
        long maxOffset = KeyValueServicePuncherStore.MAX_RANGE_SCAN_SIZE * LongMath.pow(2, maxReads - 1);
        long minOffsetForNewerEntries = maxOffset * 3 / 4;

        ThreadLocalRandom random = ThreadLocalRandom.current();
        for (int i = 0; i < 100; i++) {
            puncherStore.put(random.nextLong(2_000, 10_000L), random.nextLong(minOffsetForNewerEntries + 1, maxOffset));
        }

        for (int i = 0; i < 100; i++) {
            puncherStore.put(random.nextLong(1_000L), random.nextLong(minOffsetForNewerEntries / 3));
        }

        MillisAndMaybeTimestamp result = KeyValueServicePuncherStore.findOlder(kvs, 1_000L, maxOffset);
        assertThat(result.millis()).isLessThanOrEqualTo(maxOffset);
        assertThat(result.timestamp()).hasValueSatisfying(ts -> assertThat(ts).isLessThanOrEqualTo(1_000L));
        verify(kvs, atLeast(1)).getRange(eq(AtlasDbConstants.PUNCH_TABLE), any(RangeRequest.class), anyLong());
        verify(kvs, atMost(maxReads)).getRange(eq(AtlasDbConstants.PUNCH_TABLE), any(RangeRequest.class), anyLong());
    }

    @Test
    public void findOlderFuzzTest() {
        KeyValueService kvs = Mockito.spy(new InMemoryKeyValueService(false));
        PuncherStore puncherStore = KeyValueServicePuncherStore.create(kvs);

        int maxReads = 10;
        long maxOffset = KeyValueServicePuncherStore.MAX_RANGE_SCAN_SIZE * LongMath.pow(2, maxReads - 1);

        ThreadLocalRandom random = ThreadLocalRandom.current();
        for (int i = 0; i < 100; i++) {
            puncherStore.put(random.nextLong(10_000L), random.nextLong(maxOffset));
        }

        MillisAndMaybeTimestamp result = KeyValueServicePuncherStore.findOlder(kvs, 1_000L, maxOffset);
        assertThat(result.millis()).isLessThanOrEqualTo(maxOffset);
        if (result.timestamp().isPresent()) {
            assertThat(result).satisfies(res -> res.timestampSatisfies(ts -> ts <= 1_000L));
        }
        verify(kvs, atLeast(1)).getRange(eq(AtlasDbConstants.PUNCH_TABLE), any(RangeRequest.class), anyLong());
        verify(kvs, atMost(maxReads)).getRange(eq(AtlasDbConstants.PUNCH_TABLE), any(RangeRequest.class), anyLong());
    }

    @Test
    public void getMillisForTimestampWithinBoundsNoExactMatch() {
        KeyValueService kvs = Mockito.spy(new InMemoryKeyValueService(false));
        PuncherStore puncherStore = KeyValueServicePuncherStore.create(kvs);

        int generousReadLimit = LongMath.log2(MAX_OFFSET_FOR_BOUNDS_TESTS, RoundingMode.CEILING) * 5;
        long timestampBound = 1_000_000;
        long queryTimestamp = timestampBound * 2 / 3;

        Multimap<Long, Long> tsToMillis = generateTsToMillisMap(timestampBound, queryTimestamp, false);
        tsToMillis.forEach(puncherStore::put);

        Collection<Long> millisCandidates = tsToMillis.keySet().stream()
                .filter(ts -> ts <= queryTimestamp)
                .max(Long::compareTo)
                .map(tsToMillis::get)
                .get();
        Optional<MillisAndMaybeTimestamp> actual = KeyValueServicePuncherStore.getMillisForTimestampWithinBounds(
                kvs, queryTimestamp, null, MAX_OFFSET_FOR_BOUNDS_TESTS);

        assertThat(actual).isPresent();
        assertThat(millisCandidates).contains(actual.get().millis());
        assertThat(actual.get()).satisfies(msAndTs -> msAndTs.timestampSatisfies(ts -> ts < queryTimestamp));

        verify(kvs, atMost(generousReadLimit))
                .getRange(eq(AtlasDbConstants.PUNCH_TABLE), any(RangeRequest.class), anyLong());

        ArgumentCaptor<RangeRequest> rangeRequest = ArgumentCaptor.forClass(RangeRequest.class);
        verify(kvs).getRange(eq(AtlasDbConstants.PUNCH_TABLE), rangeRequest.capture(), eq(queryTimestamp + 1));
        assertThat(getRangeSize(rangeRequest.getValue()))
                .isBetween(1L, KeyValueServicePuncherStore.MAX_RANGE_SCAN_SIZE);
    }

    @Test
    public void getMillisForTimestampWithinBoundsWithExactMatch() {
        KeyValueService kvs = Mockito.spy(new InMemoryKeyValueService(false));
        PuncherStore puncherStore = KeyValueServicePuncherStore.create(kvs);

        int generousReadLimit = LongMath.log2(MAX_OFFSET_FOR_BOUNDS_TESTS, RoundingMode.CEILING) * 5;
        long timestampBound = 1_000_000;
        long queryTimestamp = timestampBound * 2 / 3;

        Multimap<Long, Long> tsToMillis = generateTsToMillisMap(timestampBound, queryTimestamp, true);
        tsToMillis.forEach(puncherStore::put);

        Collection<Long> millisCandidates = tsToMillis.get(queryTimestamp);

        Optional<MillisAndMaybeTimestamp> actual = KeyValueServicePuncherStore.getMillisForTimestampWithinBounds(
                kvs, queryTimestamp, null, MAX_OFFSET_FOR_BOUNDS_TESTS);

        assertThat(actual).isPresent();
        assertThat(millisCandidates).contains(actual.get().millis());
        assertThat(actual.get().timestamp()).hasValue(queryTimestamp);

        verify(kvs, atMost(generousReadLimit))
                .getRange(eq(AtlasDbConstants.PUNCH_TABLE), any(RangeRequest.class), anyLong());

        // it is still possible to have a range scan if the binary search does not stumble upon an exact match
        verify(kvs, atMost(1))
                .getRange(eq(AtlasDbConstants.PUNCH_TABLE), any(RangeRequest.class), eq(queryTimestamp + 1));
    }

    @Test
    public void getMillisForTimestampDoesNoRangeScansIfResultIsTheLowerBound() {
        KeyValueService kvs = Mockito.spy(new InMemoryKeyValueService(false));
        initializePuncherStore(PUNCHER_HISTORY, kvs);

        ImmutableMillisAndMaybeTimestamp previousResult = ImmutableMillisAndMaybeTimestamp.builder()
                .timestamp(1000)
                .millis(123123)
                .build();

        assertThat(KeyValueServicePuncherStore.getMillisForTimestampWithinBounds(
                        kvs, 1000, previousResult, MAX_OFFSET_FOR_BOUNDS_TESTS))
                .hasValue(previousResult);
        verify(kvs, never()).getRange(eq(AtlasDbConstants.PUNCH_TABLE), any(RangeRequest.class), anyLong());
    }

    private static Multimap<Long, Long> generateTsToMillisMap(long tsBound, long queryTs, boolean includeQueryTs) {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        Stream<Long> timestampStream = IntStream.range(0, 10_000)
                .mapToObj(_ignore -> random.nextLong(tsBound))
                .filter(num -> num != queryTs);

        // guarantee a result either way
        timestampStream = Streams.concat(timestampStream, Stream.of(includeQueryTs ? queryTs : 1L));

        Stream<Long> millisStream =
                IntStream.range(0, 10_000).mapToObj(_ignore -> random.nextLong(MAX_OFFSET_FOR_BOUNDS_TESTS));

        return KeyedStream.ofEntries(Streams.zip(timestampStream.sorted(), millisStream.sorted(), Maps::immutableEntry))
                .collectToSetMultimap();
    }

    private static long getRangeSize(RangeRequest rangeRequest) {
        byte[] startInclusive = rangeRequest.getStartInclusive();
        byte[] endExclusive = rangeRequest.getEndExclusive();
        EncodingUtils.flipAllBitsInPlace(startInclusive);
        EncodingUtils.flipAllBitsInPlace(endExclusive);
        return EncodingUtils.decodeUnsignedVarLong(startInclusive) - EncodingUtils.decodeUnsignedVarLong(endExclusive);
    }

    private static void assertGetOlderReturnsMillisAndNoTimestamp(KeyValueService kvs, long olderThan, long millis) {
        assertThat(KeyValueServicePuncherStore.getOlder(kvs, olderThan))
                .isEqualTo(ImmutableMillisAndMaybeTimestamp.builder()
                        .millis(millis)
                        .build());
    }

    private static void assertGetOlderReturnsMillisAndTimestamp(
            KeyValueService kvs, long olderThan, long millis, long timestamp) {
        assertThat(KeyValueServicePuncherStore.getOlder(kvs, olderThan))
                .isEqualTo(ImmutableMillisAndMaybeTimestamp.builder()
                        .millis(millis)
                        .timestamp(timestamp)
                        .build());
    }

    private static long mean(long first, long second) {
        return (first + second) / 2;
    }

    private static PuncherStore initializePuncherStore(Map<Long, Long> timestampMap) {
        return initializePuncherStore(timestampMap, new InMemoryKeyValueService(false));
    }

    private static PuncherStore initializePuncherStore(Map<Long, Long> timestampMap, KeyValueService kvs) {
        PuncherStore puncherStore = KeyValueServicePuncherStore.create(kvs);
        timestampMap.forEach(puncherStore::put);
        return puncherStore;
    }
}
