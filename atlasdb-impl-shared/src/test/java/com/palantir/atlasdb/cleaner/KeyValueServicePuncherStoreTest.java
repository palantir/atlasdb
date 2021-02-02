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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;
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
    private static final long ONE_DAY = TimeUnit.DAYS.toMillis(1);

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
    public void getMillisForTimestampIfNotPunchedBeforeEdgeCase() {
        KeyValueService kvs = new InMemoryKeyValueService(false);
        puncherStore = initializePuncherStore(PUNCHER_HISTORY, kvs);
        // Punched: (10, 100), (20, 200), (30, 300)
        // Arguments: (15, 150)
        // First ts punched before 150 is 10. 10 < 15, so we do the range scan and it returns 100.
        assertThat(KeyValueServicePuncherStore.getMillisForTimestampIfNotPunchedBefore(
                kvs, TIMESTAMP_BETWEEN_1_AND_2, WALL_CLOCK_BETWEEN_1_AND_2))
                .isEqualTo(WALL_CLOCK_1);
    }

    @Test
    public void getMillisForTimestampIfNotPunchedBeforeWhenPunchedRecently() {
        KeyValueService kvs = new InMemoryKeyValueService(false);
        puncherStore = initializePuncherStore(PUNCHER_HISTORY, kvs);
        // Punched: (10, 100), (20, 200), (30, 300)
        // Arguments: (15, 99)
        // First ts punched before 99 is Long.MIN_VALUE. Long.MIN_VALUE < 15, so we range scan and it returns 100.
        assertThat(KeyValueServicePuncherStore.getMillisForTimestampIfNotPunchedBefore(
                kvs, TIMESTAMP_BETWEEN_1_AND_2, WALL_CLOCK_1 - 1))
                .isEqualTo(WALL_CLOCK_1);
    }

    @Test
    public void getMillisForTimestampIfNotPunchedBeforeWhenLowerBoundIsNegative() {
        KeyValueService kvs = new InMemoryKeyValueService(false);
        puncherStore = initializePuncherStore(PUNCHER_HISTORY, kvs);
        // Punched: (10, 100), (20, 200), (30, 300)
        // Arguments: (15, -100)
        // Same as above, but verifying we don't error out due to negative number.
        assertThat(KeyValueServicePuncherStore.getMillisForTimestampIfNotPunchedBefore(
                kvs, TIMESTAMP_BETWEEN_1_AND_2, -100L))
                .isEqualTo(WALL_CLOCK_1);
    }

    @Test
    public void getMillisForTimestampIfNotPunchedBeforeWhenPunchedLongAgo() {
        KeyValueService kvs = Mockito.spy(new InMemoryKeyValueService(false));
        puncherStore = initializePuncherStore(PUNCHER_HISTORY, kvs);
        // Punched: (10, 100), (20, 200), (30, 300)
        // Arguments: (15, 201)
        // First ts punched before 201 is 20. 20 > 15, so we don't range scan and return 201.
        assertThat(KeyValueServicePuncherStore.getMillisForTimestampIfNotPunchedBefore(
                kvs, TIMESTAMP_BETWEEN_1_AND_2, WALL_CLOCK_2 + 1))
                .isEqualTo(WALL_CLOCK_2 + 1);
        verify(kvs, times(1)).getRange(eq(AtlasDbConstants.PUNCH_TABLE), any(RangeRequest.class), eq(Long.MAX_VALUE));
        verify(kvs, times(1)).getRange(eq(AtlasDbConstants.PUNCH_TABLE), any(RangeRequest.class), anyLong());
    }

    @Test
    public void getMillisForTimestampIfNotPunchedBeforeWhenPunchedExactlyAtLowerBound() {
        KeyValueService kvs = Mockito.spy(new InMemoryKeyValueService(false));
        puncherStore = initializePuncherStore(PUNCHER_HISTORY, kvs);
        // Punched: (10, 100), (20, 200), (30, 300)
        // Arguments: (10, 100)
        // First ts punched at 100 is 10. 10 = 10, so we don't range scan and return 100.
        assertThat(KeyValueServicePuncherStore.getMillisForTimestampIfNotPunchedBefore(kvs, TIMESTAMP_1, WALL_CLOCK_1))
                .isEqualTo(WALL_CLOCK_1);
        verify(kvs, times(1)).getRange(eq(AtlasDbConstants.PUNCH_TABLE), any(RangeRequest.class), eq(Long.MAX_VALUE));
        verify(kvs, times(1)).getRange(eq(AtlasDbConstants.PUNCH_TABLE), any(RangeRequest.class), anyLong());
    }

    @Test
    public void getOlderTest() {
        KeyValueService kvs = Mockito.spy(new InMemoryKeyValueService(false));
        puncherStore = initializePuncherStore(PUNCHER_HISTORY, kvs);
        assertGetOlderReturnsMillisAndTimestamp(kvs, 400, 300, 30);
        assertGetOlderReturnsMillisAndTimestamp(kvs, 250, 200, 20);
        assertGetOlderReturnsMillisAndTimestamp(kvs, 100, 100, 10);
        assertGetOlderReturnsMillisAndTimestamp(kvs, 50, 50, 0);

        verify(kvs, times(4)).getRange(eq(AtlasDbConstants.PUNCH_TABLE), any(RangeRequest.class), eq(Long.MAX_VALUE));
    }

    @Test
    public void findOlderWhenLatestSmaller() {
        KeyValueService kvs = Mockito.spy(new InMemoryKeyValueService(false));
        puncherStore = initializePuncherStore(PUNCHER_HISTORY, kvs);
        assertFindOlderReturnsMillisAndTimestamp(kvs, 40, 600, 300, 30);
        verify(kvs, times(1)).getRange(eq(AtlasDbConstants.PUNCH_TABLE), any(RangeRequest.class), eq(Long.MAX_VALUE));
    }

    @Test
    public void findOlderWhenLatestLargerAndStepCrossesZero() {
        KeyValueService kvs = Mockito.spy(new InMemoryKeyValueService(false));
        puncherStore = initializePuncherStore(PUNCHER_HISTORY, kvs);
        assertFindOlderReturnsMillisAndTimestamp(kvs, 25, 600, 0, 0);
        verify(kvs, times(1)).getRange(eq(AtlasDbConstants.PUNCH_TABLE), any(RangeRequest.class), eq(Long.MAX_VALUE));
    }

    @Test
    public void findOlderWhenLatestLargerAndStepFindsExactMatch() {
        KeyValueService kvs = Mockito.spy(new InMemoryKeyValueService(false));
        puncherStore = initializePuncherStore(PUNCHER_HISTORY, kvs);
        assertFindOlderReturnsMillisAndTimestamp(kvs, 20, 200 + ONE_DAY, 200, 20);
        verify(kvs, times(2)).getRange(eq(AtlasDbConstants.PUNCH_TABLE), any(RangeRequest.class), eq(Long.MAX_VALUE));
    }

    @Test
    public void findOlderWhenLatestLargerAndStepFindsSecondBest() {
        KeyValueService kvs = Mockito.spy(new InMemoryKeyValueService(false));
        puncherStore = initializePuncherStore(PUNCHER_HISTORY, kvs);
        assertFindOlderReturnsMillisAndTimestamp(kvs, 20, 150 + ONE_DAY, 100, 10);
        verify(kvs, times(2)).getRange(eq(AtlasDbConstants.PUNCH_TABLE), any(RangeRequest.class), eq(Long.MAX_VALUE));
    }

    @Test
    public void test() {
        KeyValueService kvs = Mockito.spy(new InMemoryKeyValueService(false));
        puncherStore = initializePuncherStore(PUNCHER_HISTORY, kvs);
        assertGetMillisWithNoLowerBoundEquals(kvs, 40, 1000, Optional.of(300L));
        assertGetMillisWithNoLowerBoundEquals(kvs, 30, 1000, Optional.of(300L));
        assertGetMillisWithNoLowerBoundEquals(kvs, 25, 1000, Optional.of(200L));
        assertGetMillisWithNoLowerBoundEquals(kvs, 20, 1000, Optional.of(200L));
        assertGetMillisWithNoLowerBoundEquals(kvs, 10, 1000, Optional.of(100L));
        assertGetMillisWithNoLowerBoundEquals(kvs, 5, 1000, Optional.empty());
    }

    @Test
    public void test2() {
        KeyValueService kvs = Mockito.spy(new InMemoryKeyValueService(false));
        puncherStore = initializePuncherStore(PUNCHER_HISTORY, kvs);
        assertGetMillisWithNoLowerBoundEquals(kvs, 40, 1000 + ONE_DAY, Optional.of(300L));
        assertGetMillisWithNoLowerBoundEquals(kvs, 30, 1000 + 2 * ONE_DAY, Optional.of(300L));
        assertGetMillisWithNoLowerBoundEquals(kvs, 25, 1000 + 3 * ONE_DAY, Optional.of(200L));
        assertGetMillisWithNoLowerBoundEquals(kvs, 20, 1000 + 10 * ONE_DAY, Optional.of(200L));
        assertGetMillisWithNoLowerBoundEquals(kvs, 10, 1000 + 20 * ONE_DAY, Optional.of(100L));
        assertGetMillisWithNoLowerBoundEquals(kvs, 5, 1000 + 32 * ONE_DAY, Optional.empty());
    }

    @Test
    public void test3() {
        KeyValueService kvs = Mockito.spy(new InMemoryKeyValueService(false));
        puncherStore = initializePuncherStore(PUNCHER_HISTORY, kvs);
        assertGetMillisWithNoLowerBoundEquals(kvs, 40, 15 + ONE_DAY, Optional.of(300L));
        assertGetMillisWithNoLowerBoundEquals(kvs, 30, 15 + 2 * ONE_DAY, Optional.of(300L));
        assertGetMillisWithNoLowerBoundEquals(kvs, 25, 15 + 3 * ONE_DAY, Optional.of(200L));
        assertGetMillisWithNoLowerBoundEquals(kvs, 20, 15 + 10 * ONE_DAY, Optional.of(200L));
        assertGetMillisWithNoLowerBoundEquals(kvs, 10, 15 + 20 * ONE_DAY, Optional.of(100L));
        assertGetMillisWithNoLowerBoundEquals(kvs, 5, 15 + 32 * ONE_DAY, Optional.empty());
    }

    private static void assertGetOlderReturnsMillisAndTimestamp(
            KeyValueService kvs, long olderThan, long millis,
            long timestamp) {
        assertThat(KeyValueServicePuncherStore.getOlder(kvs, olderThan)).isEqualTo(ImmutableMillisAndTimestamp.builder()
                .millis(millis)
                .timestamp(timestamp)
                .build());
    }

    private static void assertFindOlderReturnsMillisAndTimestamp(
            KeyValueService kvs, long olderThan, long upperBound, long millis, long timestamp) {
        assertThat(KeyValueServicePuncherStore.findOlder(kvs, olderThan, upperBound)).isEqualTo(
                ImmutableMillisAndTimestamp.builder()
                        .millis(millis)
                        .timestamp(timestamp)
                        .build());
    }

    private static void assertGetMillisWithNoLowerBoundEquals(KeyValueService kvs, long olderThan, long upperBound,
            Optional<Long> result) {
        assertThat(KeyValueServicePuncherStore.getMillisForTimestampWithinBounds(kvs, olderThan, null, upperBound)).isEqualTo(result);
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
