/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.timestamp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

public class TimestampRangeTest {
    private static final long SEVENTY_THREE = 73L;
    private static final long EIGHTY_TWO = 82L;

    private static final TimestampRange SEVENTY_THREE_TO_EIGHTY_TWO
            = TimestampRange.createInclusiveRange(SEVENTY_THREE, EIGHTY_TWO);

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
    public void canGetTimestampFromRangeIfItIsTheLowerBound() {
        assertThat(SEVENTY_THREE_TO_EIGHTY_TWO.getTimestampMatchingModulus(3, 10))
                .isPresent()
                .hasValue(SEVENTY_THREE);
    }

    @Test
    public void canGetTimestampFromRangeIfItIsTheUpperBound() {
        assertThat(SEVENTY_THREE_TO_EIGHTY_TWO.getTimestampMatchingModulus(2, 10))
                .isPresent()
                .hasValue(EIGHTY_TWO);
    }

    @Test
    public void canGetTimestampsFromRangeInTheMiddle() {
        assertThat(SEVENTY_THREE_TO_EIGHTY_TWO.getTimestampMatchingModulus(7, 10))
                .isPresent()
                .hasValue(77L);
        assertThat(SEVENTY_THREE_TO_EIGHTY_TWO.getTimestampMatchingModulus(8, 10))
                .isPresent()
                .hasValue(78L);
    }

    @Test
    public void canHandleMultipleValidMatches() {
        assertThat(SEVENTY_THREE_TO_EIGHTY_TWO.getTimestampMatchingModulus(1, 2))
                .isPresent()
                .satisfies(optionalLong -> {
                    long value = optionalLong.getAsLong();
                    assertThat(value).isIn(73L, 75L, 77L, 79L, 81L);
                });
        assertThat(SEVENTY_THREE_TO_EIGHTY_TWO.getTimestampMatchingModulus(0, 2))
                .isPresent()
                .satisfies(optionalLong -> {
                    long value = optionalLong.getAsLong();
                    assertThat(value).isIn(74L, 76L, 78L, 80L, 82L);
                });
    }

    @Test
    public void canHandleNegativeResidues() {
        assertThat(SEVENTY_THREE_TO_EIGHTY_TWO.getTimestampMatchingModulus(-7, 10))
                .isPresent()
                .hasValue(SEVENTY_THREE);
        assertThat(SEVENTY_THREE_TO_EIGHTY_TWO.getTimestampMatchingModulus(-5, 10))
                .isPresent()
                .hasValue(75);
    }

    @Test
    public void throwsIfModulusIsNegative() {
        assertThatThrownBy(() -> SEVENTY_THREE_TO_EIGHTY_TWO.getTimestampMatchingModulus(3, -8))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Modulus should be positive, but found -8.");
        assertThatThrownBy(() -> SEVENTY_THREE_TO_EIGHTY_TWO.getTimestampMatchingModulus(4, -2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Modulus should be positive, but found -2.");
    }

    @Test
    public void throwsIfModulusIsZero() {
        assertThatThrownBy(() -> SEVENTY_THREE_TO_EIGHTY_TWO.getTimestampMatchingModulus(0, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Modulus should be positive, but found 0.");
    }

    @Test
    public void throwsIfResidueEqualsOrExceedsModulus() {
        assertThatThrownBy(() -> SEVENTY_THREE_TO_EIGHTY_TWO.getTimestampMatchingModulus(2, 2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Absolute value of residue 2 equals or exceeds modulus 2 - no solutions");
        assertThatThrownBy(() -> SEVENTY_THREE_TO_EIGHTY_TWO.getTimestampMatchingModulus(-3, 2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageMatching("Absolute value of residue -3 equals or exceeds modulus 2 - no solutions");
    }

    @Test
    public void returnsAbsentIfTimestampRangeDoesNotContainAnyValuesMatchingModulus() {
        TimestampRange oneTimestamp = TimestampRange.createInclusiveRange(77, 77);
        assertThat(oneTimestamp.getTimestampMatchingModulus(6, 10)).isNotPresent();
    }
}
