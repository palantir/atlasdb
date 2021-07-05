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
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.hamcrest.MockitoHamcrest.longThat;

import org.junit.Before;
import org.junit.Test;

public class PersistentTimestampTests {

    public static final long UPPER_LIMIT = 1000 * 1000;
    public static final long INITIAL_TIMESTAMP = UPPER_LIMIT - 1000;
    public static final long INITIAL_REMAINING_TIMESTAMPS = UPPER_LIMIT - INITIAL_TIMESTAMP;

    private final PersistentUpperLimit upperLimit = mock(PersistentUpperLimit.class);
    private PersistentTimestamp timestamp;

    @Before
    public void before() {
        when(upperLimit.get()).thenReturn(5L);

        timestamp = new PersistentTimestamp(upperLimit, INITIAL_TIMESTAMP);
    }

    @Test
    public void shouldHandOutNonOverLappingTimestampRanges() {
        TimestampRange first = timestamp.incrementBy(10);
        TimestampRange second = timestamp.incrementBy(10);

        assertThat(first.getUpperBound()).isLessThan(second.getLowerBound());
    }

    @Test
    public void shouldHandOutRangesOfTheCorrectSize() {
        assertThat(timestamp.incrementBy(10).size()).isEqualTo(10L);
    }

    @Test
    public void shouldIncreaseUpperLimitWhenHandingOutNewTimestamps() {
        assertThat(timestamp.incrementBy(INITIAL_REMAINING_TIMESTAMPS + 10).getUpperBound())
                .isEqualTo(UPPER_LIMIT + 10);

        verify(upperLimit).increaseToAtLeast(UPPER_LIMIT + 10);
    }

    @Test
    public void canFastForwardToANewMinimumTimestamp() {
        long newMinimum = 2 * UPPER_LIMIT;
        timestamp.increaseTo(newMinimum);

        assertThat(timestamp.incrementBy(1).getLowerBound()).isEqualTo(newMinimum + 1L);
        verify(upperLimit).increaseToAtLeast(longThat(is(greaterThan(newMinimum))));
    }
}
