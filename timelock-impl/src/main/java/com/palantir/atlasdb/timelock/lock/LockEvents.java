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

package com.palantir.atlasdb.timelock.lock;

import java.util.Collection;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.collect.Iterables;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;


public class LockEvents {

    private static final Logger log = LoggerFactory.getLogger("async-lock");

    private final Timer requestTimer;
    private final Meter successfulSlowAcquisitionMeter;
    private final Meter timedOutSlowAcquisitionMeter;
    private final Meter lockExpiredMeter;

    public LockEvents(MetricRegistry metrics) {
        requestTimer = metrics.timer("lock.blocking-time");
        successfulSlowAcquisitionMeter = metrics.meter("lock.successful-slow-acquisition");
        timedOutSlowAcquisitionMeter = metrics.meter("lock.timeout-slow-acquisition");
        lockExpiredMeter = metrics.meter("lock.expired");
    }

    public void requestComplete(long blockingTimeMillis) {
        requestTimer.update(blockingTimeMillis, TimeUnit.MILLISECONDS);
    }

    public void lockExpired(UUID requestId, Collection<LockDescriptor> lockDescriptors) {
        log.warn("Lock expired",
                SafeArg.of("requestId", requestId),
                UnsafeArg.of("firstTenLockDescriptors", firstTen(lockDescriptors)));
        lockExpiredMeter.mark();
    }

    public void successfulSlowAcquisition(RequestInfo request, long acquisitionTimeMillis) {
        log.warn("Locks took a long time to acquire",
                SafeArg.of("requestId", request.id()),
                SafeArg.of("acquisitionTimeMillis", acquisitionTimeMillis),
                UnsafeArg.of("firstTenLockDescriptors", firstTen(request.lockDescriptors())),
                UnsafeArg.of("clientDescription", request.clientDescription()));
        successfulSlowAcquisitionMeter.mark();
    }

    public void timedOutSlowAcquisition(RequestInfo request, long acquisitionTimeMillis) {
        log.warn("Request timed out before obtaining locks",
                SafeArg.of("requestId", request.id()),
                SafeArg.of("acquisitionTimeMillis", acquisitionTimeMillis),
                UnsafeArg.of("firstTenLockDescriptors", firstTen(request.lockDescriptors())),
                UnsafeArg.of("clientDescription", request.clientDescription()));
        timedOutSlowAcquisitionMeter.mark();
    }

    private Iterable<LockDescriptor> firstTen(Collection<LockDescriptor> lockDescriptors) {
        return Iterables.limit(lockDescriptors, 10);
    }


    @Value.Immutable
    public interface RequestInfo {

        String EMPTY_DESCRIPTION = "<no description provided>";

        @Value.Parameter
        UUID id();

        @Value.Parameter
        String clientDescription();

        @Value.Parameter
        Set<LockDescriptor> lockDescriptors();

        static RequestInfo of(LockRequest request) {
            return ImmutableRequestInfo.of(
                    request.getRequestId(),
                    request.getClientDescription().orElse(EMPTY_DESCRIPTION),
                    request.getLockDescriptors());
        }

        static RequestInfo of(WaitForLocksRequest request) {
            return ImmutableRequestInfo.of(
                    request.getRequestId(),
                    request.getClientDescription().orElse(EMPTY_DESCRIPTION),
                    request.getLockDescriptors());
        }
    }

}
