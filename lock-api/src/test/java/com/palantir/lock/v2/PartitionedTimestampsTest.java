/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.lock.v2;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class PartitionedTimestampsTest {
    private static final int NUM_PARTITIONS = 16;

    @Test
    public void getTimestampsShouldReturnAllTimestampsWithCorrectModulus_singleTimestamp() {
        long timestamp = 123L;
        PartitionedTimestamps timestamps = ImmutablePartitionedTimestamps.builder()
                .start(timestamp)
                .interval(NUM_PARTITIONS)
                .count(1)
                .build();

        assertThat(timestamps.stream()).containsOnly(timestamp);
    }

    @Test
    public void getTimestampsShouldReturnAllTimestampsWithCorrectModulus_multipleTimestamps() {
        long lowerTimestamp = 123L;
        long middleTimestamp = lowerTimestamp + NUM_PARTITIONS;
        long upperTimestamp = middleTimestamp + NUM_PARTITIONS;

        PartitionedTimestamps timestamps = ImmutablePartitionedTimestamps.builder()
                .start(123L)
                .interval(NUM_PARTITIONS)
                .count(3)
                .build();

        assertThat(timestamps.stream()).containsOnly(lowerTimestamp, middleTimestamp, upperTimestamp);
    }
}
