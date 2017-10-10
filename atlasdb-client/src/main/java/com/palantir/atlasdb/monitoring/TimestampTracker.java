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
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.codahale.metrics.CachedGauge;
import com.codahale.metrics.Gauge;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.util.MetricsManager;

public class TimestampTracker {
    // We cache underlying calls, in case a hyper-aggressive metrics client repeatedly queries the values.
    private static final Duration CACHE_INTERVAL = Duration.ofSeconds(10L);

    private final MetricsManager metricsManager = new MetricsManager();
    private final Supplier<Long> immutableTimestampSupplier;
    private final Supplier<Long> unreadableTimestampSupplier;

    public TimestampTracker(Supplier<Long> immutableTimestampSupplier, Supplier<Long> unreadableTimestampSupplier) {
        this.immutableTimestampSupplier = immutableTimestampSupplier;
        this.unreadableTimestampSupplier = unreadableTimestampSupplier;
    }

    public void registerTimestampTrackers() {
        Map<String, Supplier<Long>> timestampTrackers = ImmutableMap.of(
                "timestamp.immutable", immutableTimestampSupplier,
                "timestamp.unreadable", unreadableTimestampSupplier);

        timestampTrackers.forEach((name, supplier) ->
                metricsManager.registerMetric(TimestampTracker.class, name, createCachingGauge(supplier)));
    }

    private <T> Gauge<T> createCachingGauge(Supplier<T> supplier) {
        return new CachedGauge<T>(CACHE_INTERVAL.getSeconds(), TimeUnit.SECONDS) {
            @Override
            protected T loadValue() {
                return supplier.get();
            }
        };
    }
}
