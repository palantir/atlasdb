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

import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.palantir.atlasdb.timelock.lock.AsyncLock;
import com.palantir.atlasdb.timelock.lock.HeldLocksCollection;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.LockWatchReferences;
import com.palantir.lock.watch.LockWatchRequest;
import com.palantir.lock.watch.LockWatchStateUpdate;

/**
 * Note on concurrency:
 * We use a fair read write lock mechanism and synchronisation as follows:
 * 1. Registering locks and unlocks requires a read lock.
 * 2. Updating the set of watched ranges requires a write lock to swap the actual reference. This ensures that, as soon
 *    as ranges are updated, any registered locks and unlocks onwards will use the updated ranges for filtering. This
 *    is necessary to guarantee that the log will reflect any changes to {@link #heldLocksCollection} that occur during
 *    execution of {@link #logOpenLocks(LockWatchRequest, UUID)}.
 * 3. Updating in {@link #addToWatches(LockWatchRequest)} is synchronised to minimise the scope of holding the write
 *    lock above while still preventing concurrent updates.
 * 4. Fairness of the lock ensures that updates are eventually granted, even in the presence of constant locks and
 *    unlocks.
 */
public class LockWatchingServiceImpl implements LockWatchingService {
    private final LockEventLog lockEventLog;
    private final HeldLocksCollection heldLocksCollection;
    private final AtomicReference<RangeSet<LockDescriptor>> ranges = new AtomicReference<>(TreeRangeSet.create());
    private final ReadWriteLock watchesLock = new ReentrantReadWriteLock(true);

    public LockWatchingServiceImpl(LockEventLog lockEventLog, HeldLocksCollection heldLocksCollection) {
        this.lockEventLog = lockEventLog;
        this.heldLocksCollection = heldLocksCollection;
    }

    @Override
    public void startWatching(LockWatchRequest locksToWatch) {
        addToWatches(locksToWatch);
        UUID requestId = UUID.randomUUID();
        logOpenLocks(locksToWatch, requestId);
        logLockWatchEvent(locksToWatch, requestId);
    }

    @Override
    public LockWatchStateUpdate getWatchStateUpdate(OptionalLong lastKnownVersion) {
        return lockEventLog.getLogDiff(lastKnownVersion);
    }

    @Override
    public void registerLock(Set<LockDescriptor> locksTakenOut, LockToken token) {
        watchesLock.readLock().lock();
        try {
            lockEventLog.logLock(locksTakenOut.stream().filter(this::hasLockWatch).collect(Collectors.toSet()), token);
        } finally {
            watchesLock.readLock().unlock();
        }
    }

    @Override
    public void registerUnlock(Set<LockDescriptor> unlocked) {
        watchesLock.readLock().lock();
        try {
        lockEventLog.logUnlock(unlocked.stream().filter(this::hasLockWatch).collect(Collectors.toSet()));
        } finally {
            watchesLock.readLock().unlock();
        }
    }

    private synchronized void addToWatches(LockWatchRequest request) {
        RangeSet<LockDescriptor> oldRanges = ranges.get();
        List<Range<LockDescriptor>> requestAsRanges = toRanges(request);
        if (oldRanges.enclosesAll(requestAsRanges)) {
            return;
        }
        RangeSet<LockDescriptor> newRanges = TreeRangeSet.create(oldRanges);
        newRanges.addAll(requestAsRanges);
        watchesLock.writeLock().lock();
        try {
            this.ranges.set(newRanges);
        } finally {
            watchesLock.writeLock().unlock();
        }
    }

    // todo(gmaretic): if this is not performant enough, consider a more tailored approach
    private void logOpenLocks(LockWatchRequest request, UUID requestId) {
        RangeSet<LockDescriptor> requestAsRangeSet = TreeRangeSet.create(toRanges(request));
        Set<LockDescriptor> openLocks = heldLocksCollection.locksHeld().stream()
                .flatMap(locksHeld -> locksHeld.getLocks().stream().map(AsyncLock::getDescriptor))
                .filter(requestAsRangeSet::contains)
                .collect(Collectors.toSet());
        lockEventLog.logOpenLocks(openLocks, requestId);
    }

    private void logLockWatchEvent(LockWatchRequest locksToWatch, UUID requestId) {
        lockEventLog.logLockWatchCreated(locksToWatch, requestId);
    }

    private boolean hasLockWatch(LockDescriptor lockDescriptor) {
        return ranges.get().contains(lockDescriptor);
    }

    private static List<Range<LockDescriptor>> toRanges(LockWatchRequest request) {
        return request.references().stream()
                .map(ref -> ref.accept(LockWatchReferences.TO_RANGES_VISITOR))
                .collect(Collectors.toList());
    }
}
