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
import java.util.UUID;
import java.util.function.Supplier;

import com.palantir.atlasdb.timelock.lock.LockEvents.RequestInfo;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.WaitForLocksRequest;

public final class LockLog {

    private static volatile Supplier<Long> slowLockThresholdMillis = () -> 10_000L;
    private static final LockEvents events = new LockEvents(AtlasDbMetrics.getMetricRegistry());

    private LockLog() { }

    public static void setSlowLockThresholdMillis(Supplier<Long> thresholdMillis) {
        slowLockThresholdMillis = thresholdMillis;
    }

    public static void registerRequest(LockRequest request, AsyncResult<?> result) {
        registerRequest(RequestInfo.of(request), result);
    }

    public static void registerRequest(WaitForLocksRequest request, AsyncResult<?> result) {
        registerRequest(RequestInfo.of(request), result);
    }

    private static void registerRequest(RequestInfo requestInfo, AsyncResult<?> result) {
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

    private static void requestComplete(
            RequestInfo requestInfo,
            AsyncResult<?> result,
            long blockingTimeMillis) {
        events.requestComplete(blockingTimeMillis);

        if (blockingTimeMillis == 0 || blockingTimeMillis < slowLockThresholdMillis.get()) {
            return;
        }

        if (result.isCompletedSuccessfully()) {
            events.successfulSlowAcquisition(requestInfo, blockingTimeMillis);
        } else if (result.isTimedOut()) {
            events.timedOutSlowAcquisition(requestInfo, blockingTimeMillis);
        }
    }

    public static void lockExpired(UUID requestId, Collection<LockDescriptor> lockDescriptors) {
        events.lockExpired(requestId, lockDescriptors);
    }

}
