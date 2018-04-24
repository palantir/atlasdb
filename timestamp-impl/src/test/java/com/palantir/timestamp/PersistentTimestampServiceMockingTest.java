/*
 * (c) Copyright 2015 Palantir Technologies Inc. All rights reserved.
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

import static java.util.concurrent.TimeUnit.SECONDS;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Test;

// Mock AvailableTimestamps to test PersistentTimestampServiceImpl.
// See also PersistentTimestampServiceTests for end-to-end style tests.
public class PersistentTimestampServiceMockingTest {

    private static final long INITIAL_TIMESTAMP = 12345L;
    private static final long TIMESTAMP = 100 * 1000;
    private static final TimestampRange SINGLE_TIMESTAMP_RANGE = TimestampRange.createInclusiveRange(TIMESTAMP, TIMESTAMP);

    private static final TimestampRange RANGE = TimestampRange.createInclusiveRange(100, 200);

    private PersistentTimestamp timestamp = mock(PersistentTimestamp.class);
    private ExecutorService executor = Executors.newSingleThreadExecutor();
    private PersistentTimestampServiceImpl timestampService = new PersistentTimestampServiceImpl(timestamp);

    @Test
    public void
    shouldDelegateFastForwardingToAvailableTimestamps() {
        timestampService.fastForwardTimestamp(INITIAL_TIMESTAMP + 1000);
        verify(timestamp).increaseTo(INITIAL_TIMESTAMP + 1000);
    }

    @Test
    public void shouldLimitRequestsTo10000Timestamps() throws InterruptedException {
        when(timestamp.incrementBy(anyLong())).thenReturn(RANGE);

        timestampService.getFreshTimestamps(20000);

        verify(timestamp).incrementBy(10000);
    }

    @Test(expected = IllegalArgumentException.class)
    public void willRejectRequestsForLessThan1Timestamp() throws InterruptedException {
        timestampService.getFreshTimestamps(0);
    }

    @Test
    public void shouldRequestTheRightTimestampFromTheAvailableTimestamps() {
        when(timestamp.incrementBy(10)).thenReturn(RANGE);

        assertThat(timestampService.getFreshTimestamps(10), is(RANGE));
    }

    @Test
    public void shouldRequestOnlyRequestASingleTimestampIfOnGetFreshTimestamp() {
        when(timestamp.incrementBy(1)).thenReturn(SINGLE_TIMESTAMP_RANGE);

        assertThat(timestampService.getFreshTimestamp(), is(TIMESTAMP));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectFastForwardToTheSentinelValue() {
        timestampService.fastForwardTimestamp(TimestampManagementService.SENTINEL_TIMESTAMP);
    }

    private void waitForExecutorToFinish() throws InterruptedException {
        executor.shutdown();
        executor.awaitTermination(10, SECONDS);
    }

}

