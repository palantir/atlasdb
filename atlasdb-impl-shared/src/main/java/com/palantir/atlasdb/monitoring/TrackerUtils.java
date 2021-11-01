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

import com.codahale.metrics.CachedGauge;
import com.codahale.metrics.Clock;
import com.codahale.metrics.Gauge;
import com.google.common.annotations.VisibleForTesting;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;

public final class TrackerUtils {
    // We cache underlying calls, in case a hyper-aggressive metrics client repeatedly queries the values.
    @VisibleForTesting
    static final Duration DEFAULT_CACHE_INTERVAL = Duration.ofSeconds(10L);

    private TrackerUtils() {
        // utility
    }

    /**
     * Creates a {@link Gauge} that caches values, and preserves previous values in the event an exception occurs.
     * Specifically, when this Gauge is queried, it invokes the underlying {@link Supplier} if it hasn't been
     * invoked in the last {@link TrackerUtils#DEFAULT_CACHE_INTERVAL}. In the event the underlying {@link Supplier}
     * throws an exception, we return the last seen value rather than propagating the exception.
     *
     * Thread safety: In the event that loadValue() is called concurrently (which can occur if, for example, a call
     * to the supplier takes more than {@link TrackerUtils#DEFAULT_CACHE_INTERVAL}) and concurrent calls are successful,
     * the value stored as the last seen value may correspond to the value returned by any call (but will correspond
     * to the value returned by one of the calls).
     *
     * @param logger to log error messages
     * @param clock to measure time
     * @param shortName to identify the name of the gauge when logging error messages
     * @param supplier returns the current value of the metric
     * @param <T> type of value the metric should have
     * @return a caching, exception-handling gauge
     */
    public static <T> Gauge<T> createCachingExceptionHandlingGauge(
            SafeLogger logger, Clock clock, String shortName, Supplier<T> supplier) {
        return createCachingReducingGauge(logger, clock, shortName, supplier, null, (_previous, current) -> current);
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
     * @param shortName to identify the name of the gauge when logging error messages
     * @param supplier returns a lower bound on the value of the metric
     * @return a caching, monotonically increasing gauge
     */
    public static Gauge<Long> createCachingMonotonicIncreasingGauge(
            SafeLogger logger, Clock clock, String shortName, Supplier<Long> supplier) {
        return createCachingReducingGauge(logger, clock, shortName, supplier, Long.MIN_VALUE, Math::max);
    }

    private static <T> Gauge<T> createCachingReducingGauge(
            SafeLogger logger,
            Clock clock,
            String shortName,
            Supplier<T> supplier,
            T initialValue,
            BinaryOperator<T> reducer) {
        return new CachedGauge<T>(clock, DEFAULT_CACHE_INTERVAL.getSeconds(), TimeUnit.SECONDS) {
            AtomicReference<T> previousValue = new AtomicReference<>(initialValue);

            @Override
            protected T loadValue() {
                try {
                    return previousValue.accumulateAndGet(supplier.get(), reducer);
                } catch (Exception e) {
                    T existingValue = previousValue.get();
                    logger.info(
                            "An exception occurred when trying to update the {} gauge for tracking purposes."
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
