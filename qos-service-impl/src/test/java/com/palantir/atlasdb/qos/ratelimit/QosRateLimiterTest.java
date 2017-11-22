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
import java.util.function.Supplier;

import org.junit.Before;
import org.junit.Test;

import com.palantir.atlasdb.qos.ratelimit.guava.RateLimiter;

public class QosRateLimiterTest {

    private static final long START_TIME_MICROS = 0L;
    private static final Supplier<Long> MAX_BACKOFF_TIME_MILLIS = () -> 10_000L;

    RateLimiter.SleepingStopwatch stopwatch = mock(RateLimiter.SleepingStopwatch.class);
    Supplier<Long> currentRate = mock(Supplier.class);
    QosRateLimiter limiter;

    @Before
    public void before() {
        when(stopwatch.readMicros()).thenReturn(START_TIME_MICROS);
        when(currentRate.get()).thenReturn(10L);

        limiter = new QosRateLimiter(stopwatch, MAX_BACKOFF_TIME_MILLIS, currentRate);
    }

    @Test
    public void doesNotLimitIfLimitIsVeryHigh() {
        when(currentRate.get()).thenReturn(Long.MAX_VALUE);

        assertThat(limiter.consumeWithBackoff(Integer.MAX_VALUE)).isEqualTo(Duration.ZERO);
        assertThat(limiter.consumeWithBackoff(Integer.MAX_VALUE)).isEqualTo(Duration.ZERO);
        assertThat(limiter.consumeWithBackoff(Integer.MAX_VALUE)).isEqualTo(Duration.ZERO);
    }

    @Test
    public void limitsOnlyWhenConsumptionExceedsLimit() {
        when(currentRate.get()).thenReturn(100L);
        limiter.consumeWithBackoff(1); // set the current time

        tickMillis(500);

        assertThat(limiter.consumeWithBackoff(25L)).isEqualTo(Duration.ZERO);
        assertThat(limiter.consumeWithBackoff(25L)).isEqualTo(Duration.ZERO);

        tickMillis(500);

        assertThat(limiter.consumeWithBackoff(20L)).isEqualTo(Duration.ZERO);

        tickMillis(500);

        assertThat(limiter.consumeWithBackoff(20L)).isEqualTo(Duration.ZERO);
        assertThat(limiter.consumeWithBackoff(20L)).isEqualTo(Duration.ZERO);
        assertThat(limiter.consumeWithBackoff(40L)).isEqualTo(Duration.ZERO);

        assertThat(limiter.consumeWithBackoff(40L)).isGreaterThan(Duration.ZERO);
    }

    @Test
    public void limitsBySleepingIfTimeIsReasonable() {
        assertThat(limiter.consumeWithBackoff(100)).isEqualTo(Duration.ZERO);
        assertThat(limiter.consumeWithBackoff(1)).isGreaterThan(Duration.ZERO);
    }

    @Test
    public void limitsByThrowingIfSleepTimeIsTooGreat() {
        limiter.consumeWithBackoff(1_000);

        assertThatThrownBy(() -> limiter.consumeWithBackoff(100))
                .isInstanceOf(RateLimitExceededException.class)
                .hasMessageContaining("Rate limited");
    }

    @Test
    public void doesNotThrowIfMaxBackoffTimeIsVeryLarge() {
        QosRateLimiter limiterWithLargeBackoffLimit = new QosRateLimiter(stopwatch, () -> Long.MAX_VALUE, () -> 10L);

        limiterWithLargeBackoffLimit.consumeWithBackoff(1_000_000_000);
        limiterWithLargeBackoffLimit.consumeWithBackoff(1_000_000_000);
    }

    @Test
    public void consumingAdditionalUnitsPenalizesFutureCallers() {
        limiter.consumeWithBackoff(1);
        limiter.recordAdjustment(25);

        assertThat(limiter.consumeWithBackoff(1)).isGreaterThan(Duration.ZERO);
    }

    @Test
    public void canConsumeBurstUnits() {
        limiter.consumeWithBackoff(100);

        // simulate 30 seconds passing with no consumption
        tickMillis(30_000);

        assertThat(limiter.consumeWithBackoff(10)).isEqualTo(Duration.ZERO);
        assertThat(limiter.consumeWithBackoff(20)).isEqualTo(Duration.ZERO);
        assertThat(limiter.consumeWithBackoff(10)).isEqualTo(Duration.ZERO);
    }

    @Test
    public void canConsumeImmediatelyAgainAfterBackoff() {
        when(currentRate.get()).thenReturn(10L);
        limiter.consumeWithBackoff(100);

        Duration timeWaited = limiter.consumeWithBackoff(20);
        assertThat(timeWaited).isGreaterThan(Duration.ZERO);

        when(stopwatch.readMicros()).thenReturn(2 * TimeUnit.NANOSECONDS.toMicros(timeWaited.toNanos()));

        assertThat(limiter.consumeWithBackoff(20)).isEqualTo(Duration.ZERO);
    }

    @Test
    public void sleepTimeIsSensible() {
        limiter.consumeWithBackoff(50);

        assertThat(limiter.consumeWithBackoff(20)).isEqualTo(Duration.ofSeconds(5));
        assertThat(limiter.consumeWithBackoff(20)).isEqualTo(Duration.ofSeconds(7));
    }

    @Test
    public void canUpdateRate() {
        // baseline
        limiter.consumeWithBackoff(20);
        assertThat(limiter.consumeWithBackoff(20)).isGreaterThan(Duration.ZERO);

        // increase to a large rate
        when(currentRate.get()).thenReturn(1000000L);
        limiter.consumeWithBackoff(1);
        tickMillis(1);

        assertThat(limiter.consumeWithBackoff(50)).isEqualTo(Duration.ZERO);
        assertThat(limiter.consumeWithBackoff(500)).isEqualTo(Duration.ZERO);

        // decrease to small rate
        when(currentRate.get()).thenReturn(10L);
        tickMillis(1000);

        limiter.consumeWithBackoff(1);
        limiter.consumeWithBackoff(20);
        assertThat(limiter.consumeWithBackoff(20)).isGreaterThan(Duration.ZERO);
    }

    private void tickMillis(long millis) {
        long now = stopwatch.readMicros();
        when(stopwatch.readMicros()).thenReturn(now + millis * 1_000);
    }
}
