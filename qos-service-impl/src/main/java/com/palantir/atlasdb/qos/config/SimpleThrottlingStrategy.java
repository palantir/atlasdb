/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.qos.config;

import java.util.Collection;
import java.util.function.Supplier;

import com.google.common.util.concurrent.RateLimiter;

import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class SimpleThrottlingStrategy implements ThrottlingStrategy {
    private static final double ONCE_EVERY_TEN_SECONDS = 0.1;
    private final RateLimiter rateLimiter;
    private double multiplier;

    public SimpleThrottlingStrategy() {
        this.rateLimiter = RateLimiter.create(ONCE_EVERY_TEN_SECONDS);
        this.multiplier = 1.0;
    }

    @Override
    public double getClientLimitMultiplier(
            Supplier<Collection<? extends CassandraHealthMetricMeasurement>> metricMeasurements) {
        if (shouldAdjust()) {
            if (cassandraIsUnhealthy(metricMeasurements)) {
                halveTheRateMultiplier();
            } else {
                // TODO(hsaraogi): increase only if the client is consuming its limit.
                increaseTheRateMultiplier();
            }
        }
        return multiplier;
    }

    private boolean shouldAdjust() {
        return rateLimiter.tryAcquire();
    }

    private synchronized void increaseTheRateMultiplier() {
        multiplier = Math.min(1.0, multiplier * 1.1);
    }

    private synchronized void halveTheRateMultiplier() {
        multiplier = Math.max(0.1, multiplier * 0.5);
    }

    private boolean cassandraIsUnhealthy(
            Supplier<Collection<? extends CassandraHealthMetricMeasurement>> metricMeasurements) {
        return metricMeasurements.get().stream()
                .anyMatch(metricMeasurement -> !metricMeasurement.isMeasurementWithinLimits());
    }
}
