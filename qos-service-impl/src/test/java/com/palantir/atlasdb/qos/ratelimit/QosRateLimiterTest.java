/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.qos.ratelimit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

public class QosRateLimiterTest {

    private static final long START_TIME_MICROS = 0L;
    private static final long MAX_BACKOFF_TIME_MILLIS = 10_000;

    RateLimiter.SleepingStopwatch stopwatch = mock(RateLimiter.SleepingStopwatch.class);
    QosRateLimiter limiter = new QosRateLimiter(stopwatch, MAX_BACKOFF_TIME_MILLIS);

    @Before
    public void before() {
        when(stopwatch.readMicros()).thenReturn(START_TIME_MICROS);
    }

    @Test
    public void doesNotLimitIfNoLimitIsSet() {
        assertThat(limiter.consumeWithBackoff(Integer.MAX_VALUE)).isEqualTo(Duration.ZERO);
        assertThat(limiter.consumeWithBackoff(Integer.MAX_VALUE)).isEqualTo(Duration.ZERO);
        assertThat(limiter.consumeWithBackoff(Integer.MAX_VALUE)).isEqualTo(Duration.ZERO);
    }

    @Test
    public void limitsBySleepingIfTimeIsReasonable() {
        limiter.updateRate(10);

        assertThat(limiter.consumeWithBackoff(100)).isEqualTo(Duration.ZERO);
        assertThat(limiter.consumeWithBackoff(1)).isGreaterThan(Duration.ZERO);
    }

    @Test
    public void limitsByThrowingIfSleepTimeIsTooGreat() {
        limiter.updateRate(10);
        limiter.consumeWithBackoff(1_000);

        assertThatThrownBy(() -> limiter.consumeWithBackoff(100))
                .isInstanceOf(RateLimitExceededException.class)
                .hasMessageContaining("Rate limited");
    }

    @Test
    public void doesNotThrowIfMaxBackoffTimeIsVeryLarge() {
        QosRateLimiter limiterWithLargeBackoffLimit = new QosRateLimiter(stopwatch, Long.MAX_VALUE);
        limiterWithLargeBackoffLimit.updateRate(10);

        limiterWithLargeBackoffLimit.consumeWithBackoff(1_000_000_000);
        limiterWithLargeBackoffLimit.consumeWithBackoff(1_000_000_000);
    }

    @Test
    public void consumingAdditionalUnitsPenalizesFutureCallers() {
        limiter.updateRate(10);

        limiter.consumeWithBackoff(1);
        limiter.recordAdjustment(100);

        assertThat(limiter.consumeWithBackoff(1)).isGreaterThan(Duration.ZERO);
    }

    @Test
    public void canConsumeBurstUnits() {
        limiter.updateRate(10);
        limiter.consumeWithBackoff(100);

        // simulate 30 seconds passing with no consumption
        when(stopwatch.readMicros()).thenReturn(TimeUnit.SECONDS.toMicros(30));

        assertThat(limiter.consumeWithBackoff(10)).isEqualTo(Duration.ZERO);
        assertThat(limiter.consumeWithBackoff(10)).isEqualTo(Duration.ZERO);
        assertThat(limiter.consumeWithBackoff(10)).isEqualTo(Duration.ZERO);
    }

    @Test
    public void canConsumeImmediatelyAgainAfterBackoff() {
        limiter.updateRate(10);
        limiter.consumeWithBackoff(100);

        Duration timeWaited = limiter.consumeWithBackoff(20);
        assertThat(timeWaited).isGreaterThan(Duration.ZERO);

        when(stopwatch.readMicros()).thenReturn(2 * TimeUnit.NANOSECONDS.toMicros(timeWaited.toNanos()));

        assertThat(limiter.consumeWithBackoff(20)).isEqualTo(Duration.ZERO);
    }

    @Test
    public void sleepTimeIsSensible() {
        limiter.updateRate(10);
        limiter.consumeWithBackoff(100);

        assertThat(limiter.consumeWithBackoff(20)).isEqualTo(Duration.ofSeconds(5));
        assertThat(limiter.consumeWithBackoff(20)).isEqualTo(Duration.ofSeconds(7));
    }
}
