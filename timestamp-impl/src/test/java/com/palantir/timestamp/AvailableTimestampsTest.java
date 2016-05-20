/**
 * Copyright 2016 Palantir Technologies
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;

public class AvailableTimestampsTest {

    public static final long LAST_RETURNED = 10L;
    public static final long UPPER_LIMIT = 100L;

    private final LastReturnedTimestamp lastReturnedTimestamp = new LastReturnedTimestamp(LAST_RETURNED);
    private final PersistentUpperLimit persistentUpperLimit = upperLimitOf(UPPER_LIMIT);

    private final AvailableTimestamps availableTimestamps = new AvailableTimestamps(
            lastReturnedTimestamp,
            persistentUpperLimit
    );

    @Test public void
    shouldContainATimestampSmallerThanTheUpperLimit() {
        assertThat(availableTimestamps.contains(UPPER_LIMIT - 10), is(true));
    }

    @Test public void
    shouldNotContainATimestampBiggerThanTheUpperLimit() {
        assertThat(availableTimestamps.contains(UPPER_LIMIT + 10), is(false));
    }

    @Test public void
    shouldContainATimestampEqualToTheUpperLimit() {
        assertThat(availableTimestamps.contains(UPPER_LIMIT), is(true));
    }

    @Test public void
    shouldBeAbleToMakeMoreTimestampsAvailable() {
        availableTimestamps.allocateMoreTimestamps();
        verify(persistentUpperLimit).increaseToAtLeast(
                lastReturnedTimestamp.get() + AvailableTimestamps.ALLOCATION_BUFFER_SIZE);
    }

    private PersistentUpperLimit upperLimitOf(long timestamp) {
        PersistentUpperLimit upperLimit = mock(PersistentUpperLimit.class);
        when(upperLimit.get()).thenReturn(timestamp);
        return upperLimit;
    }
}
