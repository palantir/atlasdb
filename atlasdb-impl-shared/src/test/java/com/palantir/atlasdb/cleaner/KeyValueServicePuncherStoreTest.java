/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.cleaner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;

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
        PUNCHER_HISTORY.forEach((key, value) -> assertThat(puncherStore.get(value)).isEqualTo(key));
    }

    @Test
    public void getTimestampForMillisReturnsLastTimestampKnownToOccurBeforeQueriedTime() {
        PUNCHER_HISTORY.forEach((key, value) -> assertThat(puncherStore.get(value + 1)).isEqualTo(key));
    }

    @Test
    public void getMillisForTimestampReturnsZeroIfQueryingTimestampBeforeFirstPunch() {
        assertThat(puncherStore.getMillisForTimestamp(Long.MIN_VALUE)).isEqualTo(0L);
        assertThat(puncherStore.getMillisForTimestamp(TIMESTAMP_1 - 1)).isEqualTo(0L);
    }

    @Test
    public void getMillisForTimestampReturnsClockTimeIfQueryingPreciseTimestamp() {
        PUNCHER_HISTORY.forEach((key, value) -> assertThat(puncherStore.getMillisForTimestamp(key)).isEqualTo(value));
    }

    @Test
    public void getMillisForTimestampReturnsLastTimeKnownToOccurBeforeQueriedTimestamp() {
        PUNCHER_HISTORY.forEach((key, value) -> assertThat(puncherStore.getMillisForTimestamp(key + 1))
                .isEqualTo(value));
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

    private static long mean(long first, long second) {
        return (first + second) / 2;
    }

    private static PuncherStore initializePuncherStore(Map<Long, Long> timestampMap) {
        PuncherStore puncherStore = KeyValueServicePuncherStore.create(new InMemoryKeyValueService(false));
        timestampMap.forEach((key, value) -> puncherStore.put(key, value));
        return puncherStore;
    }
}
