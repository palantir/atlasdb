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

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;

/**
 * A rate limiter for database queries, based on "units" of expense. This limiter strives to maintain an upper limit on
 * throughput in terms of units per second, but allows for bursts in excess of the maximum that follow periods of low
 * inactivity.
 * <p>
 * Rate limiting is achieved by sleeping prior to performing a request, or in extreme cases, throwing rate limiting
 * exceptions.
 */
public class QosRateLimiter {

    private static final double MAX_BURST_SECONDS = 5;
    private static final double UNLIMITED_RATE = Double.MAX_VALUE;

    private final long maxBackoffTimeMillis;
    private RateLimiter rateLimiter;

    public static QosRateLimiter create(long maxBackoffTimeMillis) {
        return new QosRateLimiter(RateLimiter.SleepingStopwatch.createFromSystemTimer(), maxBackoffTimeMillis);
    }

    @VisibleForTesting
    QosRateLimiter(RateLimiter.SleepingStopwatch stopwatch, long maxBackoffTimeMillis) {
        rateLimiter = new SmoothRateLimiter.SmoothBursty(
                stopwatch,
                MAX_BURST_SECONDS);

        rateLimiter.setRate(UNLIMITED_RATE);
        this.maxBackoffTimeMillis = maxBackoffTimeMillis;
    }

    /**
     * Update the allowed rate, in units per second.
     */
    public void updateRate(int unitsPerSecond) {
        rateLimiter.setRate(unitsPerSecond);
    }

    /**
     * Consumes the given {@code estimatedNumUnits}, and potentially sleeps or throws an exception if backoff is
     * required. This should be called prior to executing a query.
     *
     * @return the amount of time slept for, if any
     */
    public Duration consumeWithBackoff(int estimatedNumUnits) {
        Optional<Duration> waitTime = rateLimiter.tryAcquire(
                estimatedNumUnits,
                maxBackoffTimeMillis,
                TimeUnit.MILLISECONDS);

        if (!waitTime.isPresent()) {
            throw new RuntimeException("rate limited");
        }

        return waitTime.get();
    }

    /**
     * Records an adjustment to the original estimate of units consumed passed to {@link #consumeWithBackoff(int)}. This
     * should be called after a query returns, when the exact number of units consumed is known. This value may be
     * positive (if the original estimate was too small) or negative (if the original estimate was too large).
     */
    public void recordAdjustment(int adjustmentUnits) {
        if (adjustmentUnits > 0) {
            rateLimiter.steal(adjustmentUnits);
        }
        // TODO(nziebart): handle negative case
    }

}
