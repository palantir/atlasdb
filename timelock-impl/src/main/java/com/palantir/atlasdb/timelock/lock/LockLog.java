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
package com.palantir.atlasdb.timelock.lock;

import java.util.Collection;
import java.util.UUID;
import java.util.function.Supplier;

import com.codahale.metrics.MetricRegistry;
import com.palantir.atlasdb.timelock.lock.LockEvents.RequestInfo;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.WaitForLocksRequest;

public final class LockLog {

    private final LockEvents events;
    private final Supplier<Long> thresholdMillis;

    public LockLog(MetricRegistry metricRegistry, Supplier<Long> thresholdMillis) {
        events = new LockEvents(metricRegistry);
        this.thresholdMillis = thresholdMillis;
    }

    public void registerRequest(LockRequest request, AsyncResult<?> result) {
        registerRequest(RequestInfo.of(request), result);
    }

    public void registerRequest(WaitForLocksRequest request, AsyncResult<?> result) {
        registerRequest(RequestInfo.of(request), result);
    }

    private void registerRequest(RequestInfo requestInfo, AsyncResult<?> result) {
        if (result.isComplete()) {
            requestComplete(requestInfo, result, 0L);
            return;
        }

        long start = System.currentTimeMillis();
        result.onComplete(() -> {
            long durationMillis = System.currentTimeMillis() - start;
            requestComplete(requestInfo, result, durationMillis);
        });
    }

    private void requestComplete(
            RequestInfo requestInfo,
            AsyncResult<?> result,
            long blockingTimeMillis) {
        events.requestComplete(blockingTimeMillis);

        if (blockingTimeMillis == 0 || blockingTimeMillis < thresholdMillis.get()) {
            return;
        }

        if (result.isCompletedSuccessfully()) {
            events.successfulSlowAcquisition(requestInfo, blockingTimeMillis);
        } else if (result.isTimedOut()) {
            events.timedOutSlowAcquisition(requestInfo, blockingTimeMillis);
        }
    }

    public void lockExpired(UUID requestId, Collection<LockDescriptor> lockDescriptors) {
        events.lockExpired(requestId, lockDescriptors);
    }

}
