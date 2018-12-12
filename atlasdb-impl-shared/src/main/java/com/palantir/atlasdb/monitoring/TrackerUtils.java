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

package com.palantir.atlasdb.monitoring;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.slf4j.Logger;

import com.codahale.metrics.CachedGauge;
import com.codahale.metrics.Clock;
import com.codahale.metrics.Gauge;
import com.google.common.annotations.VisibleForTesting;
import com.palantir.logsafe.SafeArg;

public final class TrackerUtils {
    // We cache underlying calls, in case a hyper-aggressive metrics client repeatedly queries the values.
    @VisibleForTesting
    static final Duration DEFAULT_CACHE_INTERVAL = Duration.ofSeconds(10L);

    private TrackerUtils() {
        // utility
    }

    /**
     * Creates a {@link Gauge} that caches values, and is monotonically increasing.
     * Specifically, when this Gauge is queried, it invokes the underlying {@link Supplier} if it hasn't been
     * invoked in the last {@link TrackerUtils#DEFAULT_CACHE_INTERVAL}, then returns the highest seen value so far.
     * In the event the underlying {@link Supplier} throws an exception, we return the highest seen value so far,
     * rather than propagating the exception.
     *
     * @param logger to log error messages
     * @param clock to measure time
     * @param shortName used to identify the name of the gauge when logging error messages
     * @param supplier returns a lower bound on the value of the metric
     * @return a caching, monotonically increasing gauge
     */
    public static Gauge<Long> createCachingMonotonicIncreasingGauge(
            Logger logger, Clock clock, String shortName, Supplier<Long> supplier) {
        return new CachedGauge<Long>(clock, DEFAULT_CACHE_INTERVAL.getSeconds(), TimeUnit.SECONDS) {
            AtomicLong upperBound = new AtomicLong(Long.MIN_VALUE);

            @Override
            protected Long loadValue() {
                try {
                    return upperBound.accumulateAndGet(supplier.get(), Math::max);
                } catch (Exception e) {
                    long existingValue = upperBound.get();
                    logger.info("An exception occurred when trying to update the {} gauge for tracking purposes."
                                    + " Returning the last known value of {}.",
                            SafeArg.of("gaugeName", shortName),
                            SafeArg.of("existingValue", existingValue),
                            e);
                    return existingValue;
                }
            }
        };
    }
}
