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

package com.palantir.atlasdb.qos.ratelimit;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;

public class QosRateLimiter {

    private static final double MAX_BURST_SECONDS = 5;
    private static final double UNLIMITED_RATE = Double.MAX_VALUE;
    private static final int MAX_WAIT_TIME_SECONDS = 10;

    private RateLimiter rateLimiter;

    public static QosRateLimiter create() {
        return new QosRateLimiter(RateLimiter.SleepingStopwatch.createFromSystemTimer());
    }

    @VisibleForTesting
    QosRateLimiter(RateLimiter.SleepingStopwatch stopwatch) {
        rateLimiter = new SmoothRateLimiter.SmoothBursty(
                stopwatch,
                MAX_BURST_SECONDS);

        rateLimiter.setRate(UNLIMITED_RATE);
    }

    public void updateRate(int unitsPerSecond) {
        rateLimiter.setRate(unitsPerSecond);
    }

    public long consumeWithBackoff(int estimatedNumUnits) {
        Optional<Long> microsWaited = rateLimiter.tryAcquire(estimatedNumUnits, MAX_WAIT_TIME_SECONDS, TimeUnit.SECONDS);
        if (!microsWaited.isPresent()) {
            throw new RuntimeException("rate limited");
        }

        return microsWaited.get();
    }

    public void recordAdditionalConsumption(int additionalUnits) {
        rateLimiter.steal(additionalUnits);
    }

}
