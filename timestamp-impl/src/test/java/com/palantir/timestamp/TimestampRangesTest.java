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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.stream.LongStream;
import org.junit.Test;

public class TimestampRangesTest {
    private static final long SEVENTY_THREE = 73L;
    private static final long EIGHTY_TWO = 82L;

    private static final TimestampRange SEVENTY_THREE_TO_EIGHTY_TWO =
            TimestampRange.createInclusiveRange(SEVENTY_THREE, EIGHTY_TWO);

    @Test
    public void canGetTimestampFromRangeIfItIsTheLowerBound() {
        assertThat(getPartitionedTimestamps(SEVENTY_THREE_TO_EIGHTY_TWO, 3, 10)).containsExactly(SEVENTY_THREE);
    }

    @Test
    public void canGetTimestampFromRangeIfItIsTheUpperBound() {
        assertThat(getPartitionedTimestamps(SEVENTY_THREE_TO_EIGHTY_TWO, 2, 10)).containsExactly(EIGHTY_TWO);
    }

    @Test
    public void canGetTimestampsFromRangeInTheMiddle() {
        assertThat(getPartitionedTimestamps(SEVENTY_THREE_TO_EIGHTY_TWO, 7, 10)).containsExactly(77L);
        assertThat(getPartitionedTimestamps(SEVENTY_THREE_TO_EIGHTY_TWO, 8, 10)).containsExactly(78L);
    }

    @Test
    public void canHandleMultipleValidMatches() {
        assertThat(getPartitionedTimestamps(SEVENTY_THREE_TO_EIGHTY_TWO, 1, 2))
                .containsExactly(73L, 75L, 77L, 79L, 81L);

        assertThat(getPartitionedTimestamps(SEVENTY_THREE_TO_EIGHTY_TWO, 0, 2))
                .containsExactly(74L, 76L, 78L, 80L, 82L);
    }

    @Test
    public void canHandleNegativeResidues() {
        assertThat(getPartitionedTimestamps(SEVENTY_THREE_TO_EIGHTY_TWO, -7, 10))
                .containsExactly(SEVENTY_THREE);
        assertThat(getPartitionedTimestamps(SEVENTY_THREE_TO_EIGHTY_TWO, -5, 10))
                .containsExactly(75L);
    }

    @Test
    public void throwsIfModulusIsNegative() {
        assertThatThrownBy(() -> getPartitionedTimestamps(SEVENTY_THREE_TO_EIGHTY_TWO, 3, -8))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Modulus should be positive, but found -8.");
        assertThatThrownBy(() -> getPartitionedTimestamps(SEVENTY_THREE_TO_EIGHTY_TWO, 4, -2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Modulus should be positive, but found -2.");
    }

    @Test
    public void throwsIfModulusIsZero() {
        assertThatThrownBy(() -> getPartitionedTimestamps(SEVENTY_THREE_TO_EIGHTY_TWO, 0, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Modulus should be positive, but found 0.");
    }

    @Test
    public void throwsIfResidueEqualsOrExceedsModulus() {
        assertThatThrownBy(() -> getPartitionedTimestamps(SEVENTY_THREE_TO_EIGHTY_TWO, 2, 2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Absolute value of residue 2 equals or exceeds modulus 2 - no solutions");
        assertThatThrownBy(() -> getPartitionedTimestamps(SEVENTY_THREE_TO_EIGHTY_TWO, -3, 2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageMatching("Absolute value of residue -3 equals or exceeds modulus 2 - no solutions");
    }

    @Test
    public void returnsAbsentIfTimestampRangeDoesNotContainAnyValuesMatchingModulus() {
        TimestampRange oneTimestamp = TimestampRange.createInclusiveRange(77, 77);
        assertThat(getPartitionedTimestamps(oneTimestamp, 6, 10)).isEmpty();
    }

    private static LongStream getPartitionedTimestamps(TimestampRange timestampRange, int residue, int modulus) {
        return TimestampRanges.getPartitionedTimestamps(timestampRange, residue, modulus).stream();
    }
}
