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
package com.palantir.atlasdb.timelock.lock;

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.common.time.NanoTime;
import java.time.Duration;
import java.util.function.Supplier;
import org.junit.Before;
import org.junit.Test;

public class LeaseExpirationTimerTest {

    private static final long START_TIME_NANOS = 123L;

    private long currentTimeNanos = START_TIME_NANOS;
    private final Supplier<NanoTime> clock = () -> NanoTime.createForTests(currentTimeNanos);
    private LeaseExpirationTimer timer;

    @Before
    public void before() {
        currentTimeNanos = START_TIME_NANOS;
        timer = new LeaseExpirationTimer(clock);
    }

    @Test
    public void isNotImmediatelyExpired() {
        assertThat(timer.isExpired()).isFalse();
    }

    @Test
    public void isExpiredWhenAppropriateTimeHasElapsed() {
        setTime(START_TIME_NANOS + leaseDuration().toNanos() + 1L);

        assertThat(timer.isExpired()).isTrue();
    }

    @Test
    public void refreshResetsTimer() {
        setTime(START_TIME_NANOS + leaseDuration().toNanos());
        timer.refresh();

        setTime(START_TIME_NANOS + leaseDuration().toNanos() * 2);
        timer.refresh();

        assertThat(timer.isExpired()).isFalse();
    }

    private void setTime(long nanos) {
        currentTimeNanos = nanos;
    }

    private Duration leaseDuration() {
        return LockLeaseContract.SERVER_LEASE_TIMEOUT;
    }

}
