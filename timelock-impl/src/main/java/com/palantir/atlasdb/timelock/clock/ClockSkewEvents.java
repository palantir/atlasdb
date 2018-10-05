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
package com.palantir.atlasdb.timelock.clock;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;
import com.palantir.logsafe.SafeArg;

public class ClockSkewEvents {
    private static final long WARN_SKEW_THRESHOLD_NANOS = 10_000_000; // 10 ms
    private static final long ERROR_SKEW_THRESHOLD_NANOS = 50_000_000; // 50 ms

    @VisibleForTesting
    static final Duration REPRESENTATIVE_INTERVAL_SINCE_PREVIOUS_REQUEST = Duration.of(10, ChronoUnit.SECONDS);
    @VisibleForTesting
    static final Duration REPRESENTATIVE_REQUEST_DURATION = Duration.of(10, ChronoUnit.MILLIS);

    private static final double SECONDS_BETWEEN_EXCEPTION_LOGS = 600; // 10 minutes
    private static final double EXCEPTION_PERMIT_RATE = 1.0 / SECONDS_BETWEEN_EXCEPTION_LOGS;

    private final Logger log = LoggerFactory.getLogger(ClockSkewEvents.class);

    private final Histogram clockSkew;
    private final Counter exception;
    private final Counter clockWentBackwards;

    private final RateLimiter exceptionLoggingRateLimiter = RateLimiter.create(EXCEPTION_PERMIT_RATE);

    public ClockSkewEvents(MetricRegistry metricRegistry) {
        this.clockSkew = metricRegistry.histogram("clock.skew");
        this.clockWentBackwards = metricRegistry.counter("clock.went-backwards");
        this.exception = metricRegistry.counter("clock.monitor-exception");
    }

    public void clockSkew(String server, long skew, long minRequestInterval, long duration) {
        if (skew >= ERROR_SKEW_THRESHOLD_NANOS && requestHasLikelyRepresentativeSkew(minRequestInterval, duration)) {
            log.debug("Significant skew of {} ns over at least {} ns was detected on the remote server {}."
                            + " (Our request took approximately {} ns.)",
                    SafeArg.of("skew", skew),
                    SafeArg.of("minRequestInterval", minRequestInterval),
                    SafeArg.of("server", server),
                    SafeArg.of("requestDuration", duration));
        } else if (skew >= WARN_SKEW_THRESHOLD_NANOS) {
            log.debug("Skew of {} ns over at least {} ns was detected on the remote server {}."
                            + " (Our request took approximately {} ns.)",
                    SafeArg.of("skew", skew),
                    SafeArg.of("minRequestInterval", minRequestInterval),
                    SafeArg.of("server", server),
                    SafeArg.of("requestDuration", duration));
        }
        clockSkew.update(skew);
    }

    public void clockWentBackwards(String server, long amount) {
        log.info("The clock for server {} went backwards by {} nanoseconds",
                SafeArg.of("server", server),
                SafeArg.of("amountNanos", amount));

        clockWentBackwards.inc();
    }

    public void exception(Throwable throwable) {
        if (exceptionLoggingRateLimiter.tryAcquire()) {
            log.debug("ClockSkewMonitor threw an exception", throwable);
        }
        exception.inc();
    }

    @VisibleForTesting
    static boolean requestHasLikelyRepresentativeSkew(long minTimeBetweenRequests, long duration) {
        return minTimeBetweenRequests <= REPRESENTATIVE_INTERVAL_SINCE_PREVIOUS_REQUEST.toNanos()
                && duration <= REPRESENTATIVE_REQUEST_DURATION.toNanos();
    }
}
