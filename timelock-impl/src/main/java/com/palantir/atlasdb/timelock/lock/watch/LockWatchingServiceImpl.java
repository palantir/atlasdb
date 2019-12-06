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
import java.util.stream.Collectors;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.palantir.atlasdb.timelock.lock.AsyncLock;
import com.palantir.atlasdb.timelock.lock.HeldLocksCollection;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.watch.LockWatchReferences;
import com.palantir.lock.watch.LockWatchRequest;
import com.palantir.lock.watch.LockWatchStateUpdate;

public class LockWatchingServiceImpl implements LockWatchingService {
    private final LockEventLog lockEventLog;
    private final HeldLocksCollection heldLocksCollection;
    private final AtomicReference<RangeSet<LockDescriptor>> ranges = new AtomicReference<>(TreeRangeSet.create());

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
    public void registerLock(Set<LockDescriptor> locksTakenOut) {
        lockEventLog.logLock(locksTakenOut.stream().filter(this::hasLockWatch).collect(Collectors.toSet()));
    }

    @Override
    public void registerUnlock(Set<LockDescriptor> unlocked) {
        lockEventLog.logUnlock(unlocked.stream().filter(this::hasLockWatch).collect(Collectors.toSet()));
    }

    private synchronized void addToWatches(LockWatchRequest request) {
        RangeSet<LockDescriptor> oldRanges = ranges.get();
        RangeSet<LockDescriptor> newRanges = TreeRangeSet.create(oldRanges);
        newRanges.addAll(toRanges(request));
        ranges.set(newRanges);
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
