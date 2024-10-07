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

package com.palantir.atlasdb.timelock.lock.watch;

import com.google.common.collect.RangeSet;
import com.palantir.atlasdb.timelock.lock.AsyncLock;
import com.palantir.atlasdb.timelock.lock.HeldLocksCollection;
import com.palantir.atlasdb.timelock.lockwatches.BufferMetrics;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.LockEvent;
import com.palantir.lock.watch.LockRequestMetadata;
import com.palantir.lock.watch.LockWatchCreatedEvent;
import com.palantir.lock.watch.LockWatchReferences.LockWatchReference;
import com.palantir.lock.watch.LockWatchStateUpdate;
import com.palantir.lock.watch.LockWatchVersion;
import com.palantir.lock.watch.UnlockEvent;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.Unsafe;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.util.RateLimitedLogger;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class LockEventLogImpl implements LockEventLog {
    private static final SafeLogger log = SafeLoggerFactory.get(LockEventLogImpl.class);

    // Log at most 1 line every 2 minutes. Diagnostics are expected to be triggered
    // on exceptional circumstances and a one-off basis. This de-duplicates when we
    // need diagnostics on large clusters.
    private static final RateLimitedLogger diagnosticLog = new RateLimitedLogger(log, 1 / 120.0);

    private final UUID logId;
    private final ArrayLockEventSlidingWindow slidingWindow;
    private final Supplier<LockWatches> watchesSupplier;
    private final HeldLocksCollection heldLocksCollection;

    LockEventLogImpl(
            UUID logId,
            Supplier<LockWatches> watchesSupplier,
            HeldLocksCollection heldLocksCollection,
            BufferMetrics bufferMetrics) {
        this.logId = logId;
        this.slidingWindow = new ArrayLockEventSlidingWindow(1000, bufferMetrics);
        this.watchesSupplier = watchesSupplier;
        this.heldLocksCollection = heldLocksCollection;
    }

    @Override
    public synchronized LockWatchStateUpdate getLogDiff(Optional<LockWatchVersion> fromVersion) {
        return tryGetNextEvents(fromVersion).orElseGet(this::calculateSnapshot);
    }

    @Override
    public synchronized <T> ValueAndLockWatchStateUpdate<T> runTask(
            Optional<LockWatchVersion> lastKnownVersion, Supplier<T> task) {
        T t = task.get();
        LockWatchStateUpdate logDiff = getLogDiff(lastKnownVersion);
        return ValueAndLockWatchStateUpdate.of(logDiff, t);
    }

    @Override
    public synchronized void logLock(
            Set<LockDescriptor> locksTakenOut, LockToken lockToken, Optional<LockRequestMetadata> metadata) {

        slidingWindow.add(LockEvent.builder(locksTakenOut, lockToken, metadata));
    }

    @Override
    public synchronized void logUnlock(Set<LockDescriptor> locksUnlocked) {
        slidingWindow.add(UnlockEvent.builder(locksUnlocked));
    }

    @Override
    public synchronized void logLockWatchCreated(LockWatches newWatches) {
        Set<LockDescriptor> openLocks = calculateOpenLocks(newWatches.ranges());
        slidingWindow.add(LockWatchCreatedEvent.builder(newWatches.references(), openLocks));
    }

    @Override
    public void logState() {
        diagnosticLog.log(logger -> {
            logger.info(
                    "Logging state from LockEventLogImpl: {}, {}, {}, {}, {}",
                    SafeArg.of("logId", logId),
                    SafeArg.of("lastVersion", slidingWindow.lastVersion()),
                    UnsafeArg.of("watches", watchesSupplier.get()),
                    UnsafeArg.of("openLocks", calculateAllOpenLocks()),
                    UnsafeArg.of("buffer", slidingWindow.getBufferSnapshot()));
        });
    }

    private Optional<LockWatchStateUpdate> tryGetNextEvents(Optional<LockWatchVersion> fromVersion) {
        if (!fromVersion.isPresent() || !fromVersion.get().id().equals(logId)) {
            return Optional.empty();
        }

        return slidingWindow
                .getNextEvents(fromVersion.get().version())
                .map(events -> LockWatchStateUpdate.success(logId, slidingWindow.lastVersion(), events));
    }

    @Unsafe
    private LockWatchStateUpdate calculateSnapshot() {
        long lastVersion = slidingWindow.lastVersion();
        LockWatches currentWatches = watchesSupplier.get();
        Set<LockWatchReference> watches = new HashSet<>(currentWatches.references());
        Set<LockDescriptor> openLocks = calculateOpenLocks(currentWatches.ranges());
        return LockWatchStateUpdate.snapshot(logId, lastVersion, openLocks, watches);
    }

    /**
     * Iterates through all currently held locks and returns the set of all locks matching the watched ranges.
     * <p>
     * Note that the set of held locks can be modified during the execution of this method. Therefore, this method is
     * NOT guaranteed to return a consistent snapshot of the world.
     */
    private Set<LockDescriptor> calculateOpenLocks(RangeSet<LockDescriptor> watchedRanges) {
        return heldLocksCollection.locksHeld().stream()
                .flatMap(locksHeld -> locksHeld.getLocks().stream().map(AsyncLock::getDescriptor))
                .filter(watchedRanges::contains)
                .collect(Collectors.toSet());
    }

    /**
     * Returns a set of all currently held locks.
     * <p>
     * Note that the set of held locks can be modified during the execution of this method. Therefore, this method is
     * NOT guaranteed to return a consistent snapshot of the world.
     */
    private Set<LockDescriptor> calculateAllOpenLocks() {
        return heldLocksCollection.locksHeld().stream()
                .flatMap(locksHeld -> locksHeld.getLocks().stream().map(AsyncLock::getDescriptor))
                .collect(Collectors.toSet());
    }
}
