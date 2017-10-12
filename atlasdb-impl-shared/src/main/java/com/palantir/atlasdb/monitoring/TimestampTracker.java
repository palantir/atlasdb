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
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.CachedGauge;
import com.codahale.metrics.Clock;
import com.codahale.metrics.Gauge;
import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.cleaner.Cleaner;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.lock.v2.TimelockService;

public class TimestampTracker implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(TimestampTracker.class);

    // We cache underlying calls, in case a hyper-aggressive metrics client repeatedly queries the values.
    @VisibleForTesting
    static final Duration CACHE_INTERVAL = Duration.ofSeconds(10L);

    private final MetricsManager metricsManager = new MetricsManager();
    private final Clock clock;

    public TimestampTracker(Clock clock) {
        this.clock = clock;
    }

    public static TimestampTracker createWithDefaultTrackers(TimelockService timeLockService, Cleaner cleaner) {
        TimestampTracker tracker = new TimestampTracker(Clock.defaultClock());
        tracker.registerTimestampForTracking("timestamp.fresh", timeLockService::getFreshTimestamp);
        tracker.registerTimestampForTracking("timestamp.immutable", timeLockService::getImmutableTimestamp);
        tracker.registerTimestampForTracking("timestamp.unreadable", cleaner::getUnreadableTimestamp);
        return tracker;
    }

    @VisibleForTesting
    void registerTimestampForTracking(String shortName, Supplier<Long> supplier) {
        metricsManager.registerMetric(TimestampTracker.class, shortName, createCachingGauge(supplier));
    }

    private <T> Gauge<T> createCachingGauge(Supplier<T> supplier) {
        return new CachedGauge<T>(clock, CACHE_INTERVAL.getSeconds(), TimeUnit.SECONDS) {
            @Override
            protected T loadValue() {
                return supplier.get();
            }
        };
    }

    @Override
    public void close() {
        // It is usually assumed that AtlasDB clients only have one TransactionManager open at a time,
        // so deregistering everything starting with TimestampTracker is probably safe.
        metricsManager.deregisterMetricsWithPrefix(TimestampTracker.class, "");
    }
}
