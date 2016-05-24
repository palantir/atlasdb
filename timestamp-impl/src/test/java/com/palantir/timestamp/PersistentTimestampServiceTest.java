/**
 * Copyright 2015 Palantir Technologies
 * <p>
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://opensource.org/licenses/BSD-3-Clause
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.timestamp;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;

public class PersistentTimestampServiceTest {

    private static final long INITIAL_TIMESTAMP = 12345L;
    private static final long TIMESTAMP = 100 * 1000;
    private static final TimestampRange SINGLE_TIMESTAMP_RANGE = TimestampRange.createInclusiveRange(TIMESTAMP, TIMESTAMP);

    private static final TimestampRange RANGE = TimestampRange.createInclusiveRange(100, 200);

    private AvailableTimestamps availableTimestamps = mock(AvailableTimestamps.class);
    private PersistentTimestampService timestampService = new PersistentTimestampService(availableTimestamps);

    @Test
    public void
    shouldDelegateFastForwardingToAvailableTimestamps() {
        timestampService.fastForwardTimestamp(INITIAL_TIMESTAMP + 1000);
        verify(availableTimestamps).fastForwardTo(INITIAL_TIMESTAMP + 1000);
    }

    @Test
    public void shouldRequestABufferRefreshAfterEveryTimestampRequest() throws InterruptedException {
        when(availableTimestamps.handOutTimestamps(1)).thenReturn(SINGLE_TIMESTAMP_RANGE);

        timestampService.getFreshTimestamp();
        Thread.sleep(10);
        verify(availableTimestamps).refreshBuffer();
    }

    @Test
    public void shouldLimitRequestsTo10000Timestamps() throws InterruptedException {
        when(availableTimestamps.handOutTimestamps(anyLong())).thenReturn(RANGE);

        timestampService.getFreshTimestamps(20000);

        verify(availableTimestamps).handOutTimestamps(10000);
    }

    @Test(expected = IllegalArgumentException.class)
    public void willRejectRequestsForLessThan1Timestamp() throws InterruptedException {
        timestampService.getFreshTimestamps(0);
    }

    @Test
    public void shouldRequestTheRightTimestampFromTheAvailableTimestamps() {
        when(availableTimestamps.handOutTimestamps(10)).thenReturn(RANGE);

        assertThat(timestampService.getFreshTimestamps(10), is(RANGE));
    }

    @Test
    public void shouldRequestOnlyRequestASingleTimestampIfOnGetFreshTimestamp() {
        when(availableTimestamps.handOutTimestamps(1)).thenReturn(SINGLE_TIMESTAMP_RANGE);

        assertThat(timestampService.getFreshTimestamp(), is(TIMESTAMP));
    }

}

