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

import com.codahale.metrics.Clock;
import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.cleaner.api.Cleaner;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.lock.v2.TimelockService;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TimestampTracker {
    private static final Logger log = LoggerFactory.getLogger(TimestampTracker.class);

    private TimestampTracker() {}

    public static void instrumentTimestamps(
            MetricsManager metricsManager, TimelockService timeLockService, Cleaner cleaner) {
        Clock clock = Clock.defaultClock();
        registerTimestampForTracking(clock, metricsManager, "timestamp.fresh", timeLockService::getFreshTimestamp);
        registerTimestampForTracking(
                clock, metricsManager, "timestamp.immutable", timeLockService::getImmutableTimestamp);
        registerTimestampForTracking(clock, metricsManager, "timestamp.unreadable", cleaner::getUnreadableTimestamp);
    }

    @VisibleForTesting
    static void registerTimestampForTracking(
            Clock clock, MetricsManager metricsManager, String shortName, Supplier<Long> supplier) {
        metricsManager.registerMetric(
                TimestampTracker.class,
                shortName,
                TrackerUtils.createCachingMonotonicIncreasingGauge(log, clock, shortName, supplier));
    }


}
