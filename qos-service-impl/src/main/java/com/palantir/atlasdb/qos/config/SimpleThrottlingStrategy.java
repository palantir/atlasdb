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

import com.google.common.util.concurrent.RateLimiter;

public class SimpleThrottlingStrategy implements ThrottlingStrategy {
    private final RateLimiter rateLimiter;
    private double multiplier;

    public SimpleThrottlingStrategy() {
        this.rateLimiter = RateLimiter.create(0.1);
        this.multiplier = 1.0;
    }

    @Override
    public double clientLimitMultiplier(List<CassandraHealthMetricMeasurement> metricMeasurements,
            QosPriority unused) {
        if (metricMeasurements.stream()
                .anyMatch(metricMeasurement -> !metricMeasurement.isMeasurementWithinLimits())
                && rateLimiter.tryAcquire()) {
            multiplier = Math.max(0.1, multiplier * 0.5);
        } else if (rateLimiter.tryAcquire()) {
            multiplier = Math.min(1.0, multiplier * 2);
        }
        return multiplier;
    }
}
