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
package com.palantir.timestamp;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class TimestampRangeTest {
    @Test
    public void shouldHaveASizeOf1WhenCreatedWithTheSameUpperAndLowerBounds() {
        assertThat(TimestampRange.createInclusiveRange(10, 10).size()).isEqualTo(1L);
    }

    @Test
    public void shouldHaveASizeThatInludesBothEndpoints() {
        assertThat(TimestampRange.createInclusiveRange(10, 12).size()).isEqualTo(3L);
    }

    @Test
    public void shouldHaveTheSameSizeWhicheverWayRoundTheBoundsArePassed() {
        long upper = 10;
        long lower = 1;
        TimestampRange lowerUpper = TimestampRange.createInclusiveRange(lower, upper);
        TimestampRange upperLower = TimestampRange.createInclusiveRange(upper, lower);

        assertThat(lowerUpper.size()).isEqualTo(upperLower.size());
    }

    @Test
    public void shouldContainTimestampsInTheMiddleOfTheRange() {
        TimestampRange range = TimestampRange.createInclusiveRange(5, 15);
        assertThat(range.contains(8)).isTrue();
        assertThat(range.contains(12)).isTrue();
    }

    @Test
    public void shouldContainTimestampsAtRangeEndpoints() {
        TimestampRange range = TimestampRange.createInclusiveRange(5, 15);
        assertThat(range.contains(5)).isTrue();
        assertThat(range.contains(15)).isTrue();
    }

    @Test
    public void shouldNotContainTimestampsOutsideRange() {
        TimestampRange range = TimestampRange.createInclusiveRange(5, 15);
        assertThat(range.contains(4)).isFalse();
        assertThat(range.contains(16)).isFalse();
        assertThat(range.contains(237894)).isFalse();
    }
}
