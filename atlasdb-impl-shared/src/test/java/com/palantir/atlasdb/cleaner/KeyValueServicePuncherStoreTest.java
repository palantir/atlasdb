/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;

public class KeyValueServicePuncherStoreTest {
    private static final long TIMESTAMP_1 = 10L;
    private static final long TIMESTAMP_2 = 20L;
    private static final long TIMESTAMP_3 = 30L;
    private static final long TIMESTAMP_BETWEEN_1_AND_2 = 15L;

    private static final long WALL_CLOCK_1 = 100L;
    private static final long WALL_CLOCK_2 = 200L;
    private static final long WALL_CLOCK_3 = 300L;
    private static final long WALL_CLOCK_BETWEEN_1_AND_2 = 150L;
    private static final long WALL_CLOCK_BETWEEN_2_AND_3 = 250L;

    private static final ImmutableMap<Long, Long> PUNCHER_HISTORY = ImmutableMap.of(
            TIMESTAMP_1, WALL_CLOCK_1,
            TIMESTAMP_2, WALL_CLOCK_2,
            TIMESTAMP_3, WALL_CLOCK_3);

    private final PuncherStore puncherStore = KeyValueServicePuncherStore.create(new InMemoryKeyValueService(false));

    @Before
    public void setUp() {
        PUNCHER_HISTORY.entrySet().forEach(entry -> puncherStore.put(entry.getKey(), entry.getValue()));
    }

    @Test
    public void getTimestampForMillisReturnsMinValueIfQueryingClockTimeBeforeFirstPunch() {
        assertThat(puncherStore.get(WALL_CLOCK_1 - 1)).isEqualTo(Long.MIN_VALUE);
        assertThat(puncherStore.get(0L)).isEqualTo(Long.MIN_VALUE);
    }

    @Test
    public void getTimestampForMillisReturnsTimestampIfQueryingPreciseClockTime() {
        PUNCHER_HISTORY.entrySet().forEach(
                entry -> assertThat(puncherStore.get(entry.getValue())).isEqualTo(entry.getKey()));
    }

    @Test
    public void getTimestampForMillisReturnsLastTimestampKnownToOccurBeforeQueriedTime() {
        PUNCHER_HISTORY.entrySet().forEach(
                entry -> assertThat(puncherStore.get(entry.getValue() + 1)).isEqualTo(entry.getKey()));
    }

    @Test
    public void getMillisForTimestampReturnsZeroIfQueryingTimestampBeforeFirstPunch() {
        assertThat(puncherStore.getMillisForTimestamp(Long.MIN_VALUE)).isEqualTo(0L);
        assertThat(puncherStore.getMillisForTimestamp(TIMESTAMP_1 - 1)).isEqualTo(0L);
    }

    @Test
    public void getMillisForTimestampReturnsClockTimeIfQueryingPreciseTimestamp() {
        PUNCHER_HISTORY.entrySet().forEach(
                entry -> assertThat(puncherStore.getMillisForTimestamp(entry.getKey())).isEqualTo(entry.getValue()));
    }

    @Test
    public void getMillisForTimestampReturnsLastTimeKnownToOccurBeforeQueriedTimestamp() {
        PUNCHER_HISTORY.entrySet().forEach(
                entry -> assertThat(puncherStore.getMillisForTimestamp(entry.getKey() + 1))
                        .isEqualTo(entry.getValue()));
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
        puncherStore.put(TIMESTAMP_BETWEEN_1_AND_2, WALL_CLOCK_BETWEEN_2_AND_3);
        assertThat(puncherStore.getMillisForTimestamp(TIMESTAMP_2))
                .isEqualTo(WALL_CLOCK_BETWEEN_2_AND_3)
                .isNotEqualTo(WALL_CLOCK_2); // strictly speaking not needed but better for readability
    }

    @Test
    public void returnsTimestampAssociatedWithGreatestPunchedTimeEvenIfItIsNotGreatest() {
        puncherStore.put(TIMESTAMP_BETWEEN_1_AND_2, WALL_CLOCK_BETWEEN_2_AND_3);
        assertThat(puncherStore.get(WALL_CLOCK_BETWEEN_2_AND_3))
                .isEqualTo(TIMESTAMP_BETWEEN_1_AND_2)
                .isNotEqualTo(TIMESTAMP_2); // strictly speaking not needed but better for readability
    }
}
