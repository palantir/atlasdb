/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.lock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import com.palantir.common.time.Clock;

public class LeaseExpirationTimerTest {

    private static final long START_TIME_MILLIS = 123L;

    private final Clock clock = mock(Clock.class);
    private LeaseExpirationTimer timer;

    @Before
    public void before() {
        when(clock.getTimeMillis()).thenReturn(START_TIME_MILLIS);
        timer = new LeaseExpirationTimer(clock);
    }

    @Test
    public void isNotImmediatelyExpired() {
        assertThat(timer.isExpired()).isFalse();
    }

    @Test
    public void isExpiredWhenAppropriateTimeHasElapsed() {
        mockOffsetFromStartTime(LeaseExpirationTimer.LEASE_TIMEOUT_MILLIS + 1L);

        assertThat(timer.isExpired()).isTrue();
    }

    @Test
    public void refreshResetsTimer() {
        mockOffsetFromStartTime(LeaseExpirationTimer.LEASE_TIMEOUT_MILLIS);
        timer.refresh();

        mockOffsetFromStartTime(LeaseExpirationTimer.LEASE_TIMEOUT_MILLIS * 2);
        timer.refresh();

        assertThat(timer.isExpired()).isFalse();
    }

    private void mockOffsetFromStartTime(long offset) {
        when(clock.getTimeMillis()).thenReturn(START_TIME_MILLIS + offset);
    }

}
