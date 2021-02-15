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

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.debug.LockDiagnosticConfig;
import com.palantir.atlasdb.debug.LockDiagnosticInfo;
import com.palantir.atlasdb.timelock.lock.LockEvents.RequestInfo;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.client.IdentifiedLockRequest;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.logsafe.Preconditions;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

public class LockLog {

    private final LockEvents events;
    private final Optional<LockDiagnosticCollector> lockDiagnosticInfoCollector;

    public LockLog(MetricRegistry metricRegistry, Supplier<Long> thresholdMillis) {
        this(new LoggingLockEvents(metricRegistry, thresholdMillis));
    }

    private LockLog(LockEvents events) {
        this.events = events;
        this.lockDiagnosticInfoCollector = Optional.empty();
    }

    /**
     * TODO(fdesouza): Remove this once PDS-95791 is resolved.
     * @deprecated Remove this once PDS-95791 is resolved.
     */
    @Deprecated
    private LockLog(LockEvents loggingLockEvents, LockDiagnosticCollector lockDiagnosticInfoCollector) {
        this.events = new CombinedLockEvents(ImmutableList.of(loggingLockEvents, lockDiagnosticInfoCollector));
        this.lockDiagnosticInfoCollector = Optional.of(lockDiagnosticInfoCollector);
    }

    /**
     * TODO(fdesouza): Remove this once PDS-95791 is resolved.
     * @deprecated Remove this once PDS-95791 is resolved.
     */
    @Deprecated
    public LockLog withLockRequestDiagnosticCollection(LockDiagnosticConfig lockDiagnosticConfig) {
        Preconditions.checkState(!lockDiagnosticInfoCollector.isPresent(), "diagnostics are already being collected");
        return new LockLog(events, new LockDiagnosticCollector(lockDiagnosticConfig));
    }

    public void registerRequest(IdentifiedLockRequest request, AsyncResult<?> result) {
        registerRequest(RequestInfo.of(request), result);
    }

    public void registerRequest(WaitForLocksRequest request, AsyncResult<?> result) {
        registerRequest(RequestInfo.of(request), result);
    }

    /**
     * TODO(fdesouza): Remove this once PDS-95791 is resolved.
     * @deprecated Remove this once PDS-95791 is resolved.
     */
    @Deprecated
    public void registerLockImmutableTimestampRequest(UUID requestId, long timestamp, AsyncResult<?> result) {
        registerRequest(ImmutableRequestInfo.of(requestId, Long.toString(timestamp), ImmutableSet.of()), result);
    }

    private void registerRequest(RequestInfo requestInfo, AsyncResult<?> result) {
        events.registerRequest(requestInfo);
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

    private void requestComplete(RequestInfo requestInfo, AsyncResult<?> result, long blockingTimeMillis) {
        if (result.isCompletedSuccessfully()) {
            events.successfulAcquisition(requestInfo, blockingTimeMillis);
        } else if (result.isTimedOut()) {
            events.timedOut(requestInfo, blockingTimeMillis);
        }
    }

    void lockExpired(UUID requestId, Collection<LockDescriptor> lockDescriptors) {
        events.lockExpired(requestId, lockDescriptors);
    }

    void lockUnlocked(UUID requestId) {
        events.explicitlyUnlocked(requestId);
    }

    /**
     * TODO(fdesouza): Remove this once PDS-95791 is resolved.
     * @deprecated Remove this once PDS-95791 is resolved.
     */
    @Deprecated
    public Optional<LockDiagnosticInfo> getAndLogLockDiagnosticInfo(Set<UUID> startTimestamps) {
        return lockDiagnosticInfoCollector.map(
                lockDiagnosticCollector -> lockDiagnosticCollector.getAndLogCurrentState(startTimestamps));
    }

    /**
     * TODO(fdesouza): Remove this once PDS-95791 is resolved.
     * @deprecated Remove this once PDS-95791 is resolved.
     */
    @Deprecated
    void logLockDiagnosticInfo() {
        lockDiagnosticInfoCollector.ifPresent(LockDiagnosticCollector::logCurrentState);
    }
}
