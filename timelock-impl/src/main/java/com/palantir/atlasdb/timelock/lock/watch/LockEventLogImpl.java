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
import com.lmax.disruptor.RingBufferLockEventStore;
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
    private final LockEventStore slidingWindow;
    private final Supplier<LockWatches> watchesSupplier;
    private final HeldLocksCollection heldLocksCollection;

    LockEventLogImpl(
            UUID logId,
            Supplier<LockWatches> watchesSupplier,
            HeldLocksCollection heldLocksCollection,
            BufferMetrics bufferMetrics) {
        this.logId = logId;
        //        this.slidingWindow = new ArrayLockEventSlidingWindow(1000, bufferMetrics);
        this.slidingWindow = new RingBufferLockEventStore(RingBufferLockEventStore.BUFFER_SIZE, bufferMetrics);
        this.watchesSupplier = watchesSupplier;
        this.heldLocksCollection = heldLocksCollection;
    }

    // Why do we not need locking here?
    // 1. Lock events happen as part of the commit protocol BEFORE the commit timestamp is taken.
    // THEREFORE, the protocol IS OVERLY conservative as to WHEN we stop allowing caching: technically,
    // the writes from the transaction that is trying to write would only be visible to transactions with startTs >
    // commitTs of that transaction. BUT A client that sees the lock event for a particular key in the response
    // to their #startTransaction request, IMMEDIATELY flushes their cache and has to go to the KVS.
    // 2. Unlock events happen as part of the commit protocol AFTER the commit timestamp is taken.
    // AGAIN, THEREFORE the protocol is OVERLY conservative and forces transactions that start between LOCK and UNLOCK
    // to NOT USE the cache.
    //
    // Lock and unlock events ARE ALSO used for PESSIMISTIC conflict checking by Alta clients:
    // if a client sees a lock event as part of their commit can optimistically abort their commit.
    // AGAIN, because the client here is PESSIMISTIC, we don't need locking.
    //
    // Maybe, there is a possible race IFF in #runTask (which the task usually grabs some timestamps),
    // in the time between the timestamps being grabbed and log diff being calculated, a lock is grabbed,
    // commit timestamp is taken and the lock is released.
    // But even then I think that's fine?
    // * If the client is up to date, THEN by definition their fromVersion will not contain the lock/unlock sequence.
    //   So the update will contain Lock/Unlock meaning the client SHOULD NOT use their cached values.
    // * If the client is not up to date, we'll calculate a snapshot. This causes a flush of all cached values.

    @Override
    public LockWatchStateUpdate getLogDiff(Optional<LockWatchVersion> fromVersion) {
        return tryGetNextEvents(fromVersion).orElseGet(this::calculateSnapshot);
    }

    @Override
    public <T> ValueAndLockWatchStateUpdate<T> runTask(Optional<LockWatchVersion> lastKnownVersion, Supplier<T> task) {
        T t = task.get();
        LockWatchStateUpdate logDiff = getLogDiff(lastKnownVersion);
        return ValueAndLockWatchStateUpdate.of(logDiff, t);
    }

    @Override
    public void logLock(
            Set<LockDescriptor> locksTakenOut, LockToken lockToken, Optional<LockRequestMetadata> metadata) {
        slidingWindow.add(LockEvent.builder(locksTakenOut, lockToken, metadata));
    }

    @Override
    public void logUnlock(Set<LockDescriptor> locksUnlocked) {
        slidingWindow.add(UnlockEvent.builder(locksUnlocked));
    }

    @Override
    public void logLockWatchCreated(LockWatches newWatches) {
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
                .map(events -> LockWatchStateUpdate.success(logId, events.getVersion(), events.getEvents()));
    }

    @Unsafe
    private LockWatchStateUpdate calculateSnapshot() {
        // Protocol concurrency:
        // This is the only piece that I'm not sure but WE NEED to figure out the concurrency of it:
        // 1. WE ALWAYS FIRST update the heldLocksCollection, BEFORE publishing the events you can see this in the code
        //    LockAcquirer does this for locks and HeldLocks#unlockInternal does this for unlock.
        // *My intuition* that:
        // * we need to grab the lastVersion BEFORE we calculate the snapshot. This way we will not loose any events.
        // AGAIN, by the CONSERVATIVE NOTION of the protocol, if calculating the snapshot takes a long time,
        // and e.g. a lock is grabbed and released in that time (Very unlikely but still), the SNAPSHOT will have
        // the key as unlocked, but NEXT REQUEST will pull the lock and unlock events. The only result will be that
        // we'll cache the result of reads from the transaction and FLUSH it almost immediately after.
        // THIS does not break the consistency of the SYSTEM, ONLY leads to thrashing. Which we should be fine with
        // at this point in the development of this system (which is funny cause the system is probably
        // at least 3 years old, but hey ho...
        long lastVersion = slidingWindow.lastVersion();
        LockWatches currentWatches = watchesSupplier.get();
        // The way this code is structured, THIS SUPPLIER PROBABLY is not allowed to change WHILST we're running.
        // Either way, we should double check that, but I'm going to assume this is fine as is and we can establish that
        // later.
        // *Intuitively* it feels like, since #calculateOpenLocks is taking the currentWatches,
        // the snapshot will be self-consistent and safe, so we don't need to necessarily hold a lock on the watches.
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
