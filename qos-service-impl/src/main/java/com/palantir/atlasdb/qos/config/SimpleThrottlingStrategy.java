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
package com.palantir.atlasdb.qos.config;

import java.util.List;
import java.util.function.Supplier;

import com.palantir.atlasdb.qos.ratelimit.guava.RateLimiter;

public class SimpleThrottlingStrategy implements ThrottlingStrategy {

    private static final double ONCE_EVERY_HUNDRED_SECONDS = 0.01;
    private final RateLimiter rateLimiter;
    private double multiplier;

    public SimpleThrottlingStrategy() {
        this.rateLimiter = RateLimiter.create(ONCE_EVERY_HUNDRED_SECONDS);
        this.multiplier = 1.0;
    }

    @Override
    public double getClientLimitMultiplier(Supplier<List<CassandraHealthMetricMeasurement>> metricMeasurements,
            QosPriority unused) {
        if (shouldAdjust()) {
            if (cassandraIsUnhealthy(metricMeasurements.get())) {
                multiplier = halveTheRateMultiplier();
            } else {
                // TODO(hsaraogi): increase only in the qosMetrics imply that the client is consuming its limit.
                multiplier = increaseTheRateMultiplier();
            }
        }
        return multiplier;
    }

    private boolean shouldAdjust() {
        return rateLimiter.tryAcquire();
    }

    private double increaseTheRateMultiplier() {
        return Math.min(2.0, multiplier * 1.1);
    }

    private double halveTheRateMultiplier() {
        return Math.max(0.1, multiplier * 0.5);
    }

    private boolean cassandraIsUnhealthy(List<CassandraHealthMetricMeasurement> metricMeasurements) {
        return metricMeasurements.stream()
                .anyMatch(metricMeasurement -> !metricMeasurement.isMeasurementWithinLimits());
    }
}
