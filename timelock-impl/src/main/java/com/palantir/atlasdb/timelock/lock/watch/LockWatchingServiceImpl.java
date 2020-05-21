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

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.palantir.atlasdb.timelock.api.LockWatchRequest;
import com.palantir.atlasdb.timelock.lock.HeldLocksCollection;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.IdentifiedVersion;
import com.palantir.lock.watch.LockWatchReferences;
import com.palantir.lock.watch.LockWatchReferences.LockWatchReference;
import com.palantir.lock.watch.LockWatchStateUpdate;

/**
 * Note on concurrency:
 * We use a fair read write lock mechanism and synchronisation as follows:
 * 1. Registering locks and unlocks requires a read lock.
 * 2. Updating watches requires a write lock to swap the actual reference. This ensures that, as soon as an update is
 *    made, any registered locks and unlocks onwards will use updated ranges for filtering. This is necessary to
 *    guarantee that the log will contain any locks/unlocks of newly watched locks; see
 *    {@link LockEventLogImpl#calculateOpenLocks} for more details.
 * 3. Updating in {@link #addToWatches(LockWatchRequest)} is synchronised to minimise the scope of holding the write
 *    lock above while still preventing concurrent updates.
 * 4. Fairness of the lock ensures that updates are eventually granted, even in the presence of constant locks and
 *    unlocks.
 */
@SuppressWarnings("UnstableApiUsage")
public class LockWatchingServiceImpl implements LockWatchingService {
    private final LockEventLog lockEventLog;
    private final AtomicReference<LockWatches> watches = new AtomicReference<>(LockWatches.create());
    private final ReadWriteLock watchesLock = new ReentrantReadWriteLock(true);

    public LockWatchingServiceImpl(HeldLocksCollection heldLocksCollection) {
        this(UUID.randomUUID(), heldLocksCollection);
    }

    @VisibleForTesting
    LockWatchingServiceImpl(UUID logId, HeldLocksCollection heldLocksCollection) {
        this.lockEventLog = new LockEventLogImpl(logId, watches::get, heldLocksCollection);
    }

    @Override
    public void startWatching(LockWatchRequest locksToWatch) {
        Optional<LockWatches> changes = addToWatches(locksToWatch);
        changes.ifPresent(this::logLockWatchEvent);
    }

    @Override
    public LockWatchStateUpdate getWatchStateUpdate(Optional<IdentifiedVersion> lastKnownVersion) {
        return lockEventLog.getLogDiff(lastKnownVersion);
    }

    @Override
    public LockWatchStateUpdate getWatchStateUpdate(Optional<IdentifiedVersion> lastKnownVersion, long endVersion) {
        return lockEventLog.getLogDiff(lastKnownVersion, endVersion);
    }

    /**
     * Warning: this will block all lock and unlock requests until the task is done. Improper use of this method can
     * result in a deadlock.
     */
    @Override
    public <T> ValueAndVersion<T> runTaskAndAtomicallyReturnLockWatchVersion(Supplier<T> task) {
        return lockEventLog.runTaskAndAtomicallyReturnVersion(task);
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

    private synchronized Optional<LockWatches> addToWatches(LockWatchRequest request) {
        LockWatches oldWatches = watches.get();
        Optional<LockWatches> newWatches = filterNewWatches(request, oldWatches);
        if (newWatches.isPresent()) {
            LockWatches updatedWatches = LockWatches.merge(oldWatches, newWatches.get());
            watchesLock.writeLock().lock();
            try {
                watches.set(updatedWatches);
            } finally {
                watchesLock.writeLock().unlock();
            }
        }
        return newWatches;
    }

    private Optional<LockWatches> filterNewWatches(LockWatchRequest request, LockWatches oldWatches) {
        Set<LockWatchReference> newRefs = new HashSet<>();
        RangeSet<LockDescriptor> newRanges = TreeRangeSet.create();
        for (LockWatchReference singleReference : request.getReferences()) {
            Range<LockDescriptor> referenceAsRange = singleReference.accept(LockWatchReferences.TO_RANGES_VISITOR);
            if (!oldWatches.ranges().encloses(referenceAsRange)) {
                newRefs.add(singleReference);
                newRanges.add(referenceAsRange);
            }
        }
        return newRefs.isEmpty() ? Optional.empty() : Optional.of(ImmutableLockWatches.of(newRefs, newRanges));
    }

    private void logLockWatchEvent(LockWatches newWatches) {
        lockEventLog.logLockWatchCreated(newWatches);
    }

    private boolean hasLockWatch(LockDescriptor lockDescriptor) {
        return watches.get().ranges().contains(lockDescriptor);
    }
}
