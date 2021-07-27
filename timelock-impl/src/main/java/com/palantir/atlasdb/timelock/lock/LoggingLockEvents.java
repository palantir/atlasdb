/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.timelock.lock;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.collect.Iterables;
import com.palantir.lock.LockDescriptor;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class LoggingLockEvents implements LockEvents {

    private static final SafeLogger log = SafeLoggerFactory.get("async-lock");

    private final Timer requestTimer;
    private final Meter successfulSlowAcquisitionMeter;
    private final Meter timedOutSlowAcquisitionMeter;
    private final Meter lockExpiredMeter;
    private final Supplier<Long> thresholdMillis;

    LoggingLockEvents(MetricRegistry metrics, Supplier<Long> thresholdMillis) {
        this.requestTimer = metrics.timer("lock.blocking-time");
        this.successfulSlowAcquisitionMeter = metrics.meter("lock.successful-slow-acquisition");
        this.timedOutSlowAcquisitionMeter = metrics.meter("lock.timeout-slow-acquisition");
        this.lockExpiredMeter = metrics.meter("lock.expired");
        this.thresholdMillis = thresholdMillis;
    }

    @Override
    public void registerRequest(RequestInfo request) {
        // do nothing
    }

    @Override
    public void timedOut(RequestInfo request, long acquisitionTimeMillis) {
        requestTimer.update(acquisitionTimeMillis, TimeUnit.MILLISECONDS);
        if (!isSlowAcquisition(acquisitionTimeMillis)) {
            return;
        }

        log.warn(
                "Request timed out before obtaining locks",
                SafeArg.of("requestId", request.id()),
                SafeArg.of("acquisitionTimeMillis", acquisitionTimeMillis),
                UnsafeArg.of("firstTenLockDescriptors", firstTen(request.lockDescriptors())),
                UnsafeArg.of("clientDescription", request.clientDescription()));
        timedOutSlowAcquisitionMeter.mark();
    }

    @Override
    public void successfulAcquisition(RequestInfo request, long acquisitionTimeMillis) {
        requestTimer.update(acquisitionTimeMillis, TimeUnit.MILLISECONDS);
        if (!isSlowAcquisition(acquisitionTimeMillis)) {
            return;
        }

        log.warn(
                "Locks took a long time to acquire",
                SafeArg.of("requestId", request.id()),
                SafeArg.of("acquisitionTimeMillis", acquisitionTimeMillis),
                UnsafeArg.of("firstTenLockDescriptors", firstTen(request.lockDescriptors())),
                UnsafeArg.of("clientDescription", request.clientDescription()));
        successfulSlowAcquisitionMeter.mark();
    }

    @Override
    public void lockExpired(UUID requestId, Collection<LockDescriptor> lockDescriptors) {
        log.warn(
                "Lock expired",
                SafeArg.of("requestId", requestId),
                UnsafeArg.of("firstTenLockDescriptors", firstTen(lockDescriptors)));
        lockExpiredMeter.mark();
    }

    @Override
    public void explicitlyUnlocked(UUID requestId) {
        // do nothing
    }

    private boolean isSlowAcquisition(long acquisitionTimeMillis) {
        return acquisitionTimeMillis != 0 && acquisitionTimeMillis >= thresholdMillis.get();
    }

    private static Iterable<LockDescriptor> firstTen(Collection<LockDescriptor> lockDescriptors) {
        return Iterables.limit(lockDescriptors, 10);
    }
}
