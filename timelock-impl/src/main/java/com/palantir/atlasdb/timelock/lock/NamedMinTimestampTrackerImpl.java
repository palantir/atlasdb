/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

import com.palantir.atlasdb.timelock.timestampleases.TimestampLeaseMetrics;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.concurrent.GuardedBy;

final class NamedMinTimestampTrackerImpl implements NamedMinTimestampTracker {
    private final String timestampName;
    private final AtomicInteger numLocksHeld;

    @GuardedBy("this")
    private final SortedMap<Long, UUID> holdersByTimestamp = new TreeMap<>();

    private NamedMinTimestampTrackerImpl(String timestampName, AtomicInteger numLocksHeld) {
        this.timestampName = timestampName;
        this.numLocksHeld = numLocksHeld;
    }

    static NamedMinTimestampTracker create(String timestampName, TimestampLeaseMetrics metrics) {
        AtomicInteger numLocksHeld = new AtomicInteger();
        metrics.locksHeld().name(timestampName).build(numLocksHeld::get);

        return new NamedMinTimestampTrackerImpl(timestampName, numLocksHeld);
    }

    @Override
    public synchronized void lock(long timestamp, UUID requestId) {
        boolean wasAdded = holdersByTimestamp.putIfAbsent(timestamp, requestId) == null;
        if (!wasAdded) {
            throw new SafeIllegalStateException(
                    "A request attempted to lock a timestamp that was already locked",
                    SafeArg.of("timestamp", timestamp),
                    SafeArg.of("requestId", requestId),
                    SafeArg.of("currentHolder", holdersByTimestamp.get(timestamp)));
        }
        numLocksHeld.incrementAndGet();
    }

    @Override
    public synchronized void unlock(long timestamp, UUID requestId) {
        boolean wasRemoved = holdersByTimestamp.remove(timestamp, requestId);
        if (!wasRemoved) {
            throw new SafeIllegalStateException(
                    "A request attempted to unlock a timestamp that was not locked or was locked by another request",
                    SafeArg.of("timestamp", timestamp),
                    SafeArg.of("requestId", requestId),
                    SafeArg.of("currentHolder", holdersByTimestamp.get(timestamp)));
        }
        numLocksHeld.decrementAndGet();
    }

    @Override
    public synchronized Optional<Long> getMinimumTimestamp() {
        if (holdersByTimestamp.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(holdersByTimestamp.firstKey());
    }

    @Override
    public LockDescriptor getDescriptor(long timestamp) {
        return StringLockDescriptor.of(timestampName + ":" + timestamp);
    }
}
