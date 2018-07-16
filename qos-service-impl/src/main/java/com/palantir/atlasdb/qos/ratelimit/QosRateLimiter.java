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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.math.LongMath;
import com.palantir.atlasdb.qos.ratelimit.guava.RateLimiter;
import com.palantir.atlasdb.qos.ratelimit.guava.SmoothRateLimiter;
import com.palantir.logsafe.SafeArg;
import com.palantir.remoting.api.errors.QosException;

/**
 * A rate limiter for database queries, based on "units" of expense. This limiter strives to maintain an upper limit on
 * throughput in terms of units per second, but allows for bursts in excess of the maximum that follow periods of low
 * inactivity.
 * <p>
 * Rate limiting is achieved by sleeping prior to performing a request, or in extreme cases, throwing rate limiting
 * exceptions.
 */
public class QosRateLimiter implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(QosRateLimiter.class);

    private static final long MAX_BURST_SECONDS = 5;
    private static final String RATE_UPDATE_ERROR_MESSAGE = "Could not refresh the Qos rate."
            + " This can happen if the Qos Service is unreachable."
            + " Extended periods of being unable to refresh will hinder QoS of all clients.";

    @VisibleForTesting
    static final int RATE_UPDATE_INTERVAL_IN_SECONDS = 2;

    private final Supplier<Long> maxBackoffTimeMillis;
    private final String rateLimiterName;
    private final Supplier<Long> unitsPerSecond;
    private final RateLimiter.SleepingStopwatch stopwatch;
    private final ScheduledExecutorService executorService;
    private final ScheduledFuture<?> scheduledFuture;

    private volatile RateLimiter rateLimiter;
    private volatile long currentRate;
    private final long defaultRate;

    public static QosRateLimiter create(
            Supplier<Long> maxBackoffTimeMillis,
            Supplier<Long> unitsPerSecond,
            String rateLimiterType,
            ScheduledExecutorService executorService,
            long defaultRate) {
        return new QosRateLimiter(RateLimiter.SleepingStopwatch.createFromSystemTimer(), maxBackoffTimeMillis,
                unitsPerSecond, rateLimiterType, executorService, defaultRate);
    }

    @VisibleForTesting
    QosRateLimiter(RateLimiter.SleepingStopwatch stopwatch,
            Supplier<Long> maxBackoffTimeMillis,
            Supplier<Long> unitsPerSecond,
            String rateLimiterName,
            ScheduledExecutorService executorService,
            long defaultRate) {
        this.stopwatch = stopwatch;
        this.unitsPerSecond = unitsPerSecond;
        this.maxBackoffTimeMillis = maxBackoffTimeMillis;
        this.rateLimiterName = rateLimiterName;
        this.defaultRate = defaultRate;
        this.executorService = executorService;
        createRateLimiterAtomically(getUpdatedRate());

        scheduledFuture = executorService
                .scheduleWithFixedDelay(() -> {
                    try {
                        updateRateIfNeeded();
                    } catch (Throwable t) {
                        log.info(RATE_UPDATE_ERROR_MESSAGE, t);
                    }
                }, 0, RATE_UPDATE_INTERVAL_IN_SECONDS, TimeUnit.SECONDS);
    }

    @Override
    public void close() {
        scheduledFuture.cancel(true);
        executorService.shutdown();
    }

    /**
     * Consumes the given {@code estimatedNumUnits}, and potentially sleeps or throws an exception if backoff is
     * required. This should be called prior to executing a query.
     *
     * @return the amount of time slept for, if any
     */
    public Duration consumeWithBackoff(long estimatedNumUnits) {
        Optional<Duration> waitTime = rateLimiter.tryAcquire(
                estimatedNumUnits,
                maxBackoffTimeMillis.get(),
                TimeUnit.MILLISECONDS);

        if (!waitTime.isPresent()) {
            throw QosException.throttle();
        }

        return waitTime.get();
    }

    /**
     * The RateLimiter's rate requires a lock acquisition to read, and is returned as a double. To avoid
     * overhead and double comparisons, we maintain the current rate ourselves.
     */
    private void updateRateIfNeeded() {
        long updatedRate = getUpdatedRate();
        if (currentRate != updatedRate) {
            createRateLimiterAtomically(updatedRate);
        }
    }

    private long getUpdatedRate() {
        try {
            return unitsPerSecond.get();
        } catch (Exception e) {
            log.info(RATE_UPDATE_ERROR_MESSAGE, e);
            return returnPreviousOrDefaultRate();
        }
    }

    private long returnPreviousOrDefaultRate() {
        if (currentRate > 0) {
            return currentRate;
        } else {
            return defaultRate;
        }
    }

    /**
     * Guava's RateLimiter has strange behavior around updating the rate. Namely, if you set the rate very small and ask
     * for a large number of permits, you will end up having to wait until that small rate is satisfied before acquiring
     * more, even if you update the rate to something very large. So, we just create a new rate limiter if the rate
     * changes.
     */
    private synchronized void createRateLimiterAtomically(long rate) {
        currentRate = rate;
        rateLimiter = new SmoothRateLimiter.SmoothBursty(stopwatch, MAX_BURST_SECONDS);
        rateLimiter.setRate(currentRate);

        log.info("Units per second set to {} for rate limiter {}",
                SafeArg.of("unitsPerSecond", currentRate),
                SafeArg.of("rateLimiterName", rateLimiterName));
    }

    /**
     * Records an adjustment to the original estimate of units consumed passed to {@link #consumeWithBackoff}. This
     * should be called after a query returns, when the exact number of units consumed is known. This value may be
     * positive (if the original estimate was too small) or negative (if the original estimate was too large).
     */
    public void recordAdjustment(long adjustmentUnits) {
        if (adjustmentUnits > 0) {
            rateLimiter.steal(adjustmentUnits);
        } else if (adjustmentUnits < 0) {
            rateLimiter.returnPermits(safeNegative(adjustmentUnits));
        }
    }

    private long safeNegative(long adjustmentUnits) {
        try {
            return LongMath.checkedMultiply(-1, adjustmentUnits);
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

}
