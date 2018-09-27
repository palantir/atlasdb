/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.transaction.timestamp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

import com.palantir.timestamp.TimestampRange;

public class TimestampRangesTest {
    private static final long SEVENTY_THREE = 73L;
    private static final long EIGHTY_TWO = 82L;

    private static final TimestampRange SEVENTY_THREE_TO_EIGHTY_TWO
            = TimestampRange.createInclusiveRange(SEVENTY_THREE, EIGHTY_TWO);

    @Test
    public void canGetTimestampFromRangeIfItIsTheLowerBound() {
        assertThat(TimestampRanges.getTimestampMatchingModulus(SEVENTY_THREE_TO_EIGHTY_TWO, 3, 10))
                .isPresent()
                .hasValue(SEVENTY_THREE);
    }

    @Test
    public void canGetTimestampFromRangeIfItIsTheUpperBound() {
        assertThat(TimestampRanges.getTimestampMatchingModulus(SEVENTY_THREE_TO_EIGHTY_TWO, 2, 10))
                .isPresent()
                .hasValue(EIGHTY_TWO);
    }

    @Test
    public void canGetTimestampsFromRangeInTheMiddle() {
        assertThat(TimestampRanges.getTimestampMatchingModulus(SEVENTY_THREE_TO_EIGHTY_TWO, 7, 10))
                .isPresent()
                .hasValue(77L);
        assertThat(TimestampRanges.getTimestampMatchingModulus(SEVENTY_THREE_TO_EIGHTY_TWO, 8, 10))
                .isPresent()
                .hasValue(78L);
    }

    @Test
    public void canHandleMultipleValidMatches() {
        assertThat(TimestampRanges.getTimestampMatchingModulus(SEVENTY_THREE_TO_EIGHTY_TWO, 1, 2))
                .isPresent()
                .satisfies(optionalLong -> {
                    long value = optionalLong.getAsLong();
                    assertThat(value).isIn(73L, 75L, 77L, 79L, 81L);
                });
        assertThat(TimestampRanges.getTimestampMatchingModulus(SEVENTY_THREE_TO_EIGHTY_TWO, 0, 2))
                .isPresent()
                .satisfies(optionalLong -> {
                    long value = optionalLong.getAsLong();
                    assertThat(value).isIn(74L, 76L, 78L, 80L, 82L);
                });
    }

    @Test
    public void throwsIfResidueExceedsOrEqualsModulus() {
        assertThatThrownBy(() -> TimestampRanges.getTimestampMatchingModulus(SEVENTY_THREE_TO_EIGHTY_TWO, 2, 2))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> TimestampRanges.getTimestampMatchingModulus(SEVENTY_THREE_TO_EIGHTY_TWO, 3, 2))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void throwsIfTimestampRangeDoesNotContainAnyValuesMatchingModulus() {
        TimestampRange oneTimestamp = TimestampRange.createInclusiveRange(77, 77);
        assertThat(TimestampRanges.getTimestampMatchingModulus(oneTimestamp, 6, 10)).isNotPresent();
    }
}