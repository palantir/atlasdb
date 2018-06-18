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

package com.palantir.atlasdb.monitoring;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.CachedGauge;
import com.codahale.metrics.Clock;
import com.codahale.metrics.Gauge;
import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.cleaner.api.Cleaner;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.lock.v2.TimelockService;
import com.palantir.logsafe.SafeArg;

public final class TimestampTracker {
    private static final Logger log = LoggerFactory.getLogger(TimestampTracker.class);

    // We cache underlying calls, in case a hyper-aggressive metrics client repeatedly queries the values.
    @VisibleForTesting
    static final Duration CACHE_INTERVAL = Duration.ofSeconds(10L);

    private TimestampTracker() {}

    public static void instrumentTimestamps(
            MetricsManager metricsManager, TimelockService timeLockService, Cleaner cleaner) {
        Clock clock = Clock.defaultClock();
        registerTimestampForTracking(clock, metricsManager, "timestamp.fresh", timeLockService::getFreshTimestamp);
        registerTimestampForTracking(
                clock, metricsManager, "timestamp.immutable", timeLockService::getImmutableTimestamp);
        registerTimestampForTracking(clock, metricsManager,"timestamp.unreadable", cleaner::getUnreadableTimestamp);
    }

    @VisibleForTesting
    static void registerTimestampForTracking(
            Clock clock, MetricsManager metricsManager, String shortName, Supplier<Long> supplier) {
        metricsManager.registerMetric(
                TimestampTracker.class,
                shortName,
                createCachingTimestampGauge(clock, shortName, supplier));
    }

    private static Gauge<Long> createCachingTimestampGauge(Clock clock, String shortName, Supplier<Long> supplier) {
        return new CachedGauge<Long>(clock, CACHE_INTERVAL.getSeconds(), TimeUnit.SECONDS) {
            AtomicLong upperBound = new AtomicLong(Long.MIN_VALUE);

            @Override
            protected Long loadValue() {
                try {
                    // Note that this gauge is only an approximation, because some timestamps can go backwards.
                    return upperBound.accumulateAndGet(supplier.get(), Math::max);
                } catch (Exception e) {
                    long timestampToReturn = upperBound.get();
                    log.info("An exception occurred when trying to update the {} timestamp for tracking purposes."
                                    + " Returning the last known value of {}.",
                            SafeArg.of("timestampName", shortName),
                            SafeArg.of("timestamp", timestampToReturn),
                            e);
                    return timestampToReturn;
                }
            }
        };
    }
}
