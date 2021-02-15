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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;

// Mock AvailableTimestamps to test PersistentTimestampServiceImpl.
// See also PersistentTimestampServiceTests for end-to-end style tests.
public class PersistentTimestampServiceMockingTest {

    private static final long INITIAL_TIMESTAMP = 12345L;
    private static final long TIMESTAMP = 100 * 1000;
    private static final TimestampRange SINGLE_TIMESTAMP_RANGE =
            TimestampRange.createInclusiveRange(TIMESTAMP, TIMESTAMP);

    private static final TimestampRange RANGE = TimestampRange.createInclusiveRange(100, 200);

    private PersistentTimestamp timestamp = mock(PersistentTimestamp.class);
    private PersistentTimestampServiceImpl timestampService = new PersistentTimestampServiceImpl(timestamp);

    @Test
    public void shouldDelegateFastForwardingToAvailableTimestamps() {
        timestampService.fastForwardTimestamp(INITIAL_TIMESTAMP + 1000);
        verify(timestamp).increaseTo(INITIAL_TIMESTAMP + 1000);
    }

    @Test
    public void shouldLimitRequestsTo10000Timestamps() {
        when(timestamp.incrementBy(anyLong())).thenReturn(RANGE);

        timestampService.getFreshTimestamps(20000);

        verify(timestamp).incrementBy(10000);
    }

    @Test(expected = IllegalArgumentException.class)
    public void willRejectRequestsForLessThan1Timestamp() {
        timestampService.getFreshTimestamps(0);
    }

    @Test
    public void shouldRequestTheRightTimestampFromTheAvailableTimestamps() {
        when(timestamp.incrementBy(10)).thenReturn(RANGE);

        assertThat(timestampService.getFreshTimestamps(10)).isEqualTo(RANGE);
    }

    @Test
    public void shouldRequestOnlyRequestASingleTimestampIfOnGetFreshTimestamp() {
        when(timestamp.incrementBy(1)).thenReturn(SINGLE_TIMESTAMP_RANGE);

        assertThat(timestampService.getFreshTimestamp()).isEqualTo(TIMESTAMP);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectFastForwardToTheSentinelValue() {
        timestampService.fastForwardTimestamp(TimestampManagementService.SENTINEL_TIMESTAMP);
    }
}
