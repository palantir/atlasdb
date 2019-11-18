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

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;

import com.palantir.atlasdb.timelock.lock.HeldLocksCollection;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.LockWatchRequest;
import com.palantir.lock.watch.LockWatchStateUpdate;

public class LockWatchingServiceImpl implements LockWatchingService {
    private final LockEventLog lockEventLog;
    private final HeldLocksCollection heldLocksCollection;
    private final ConcurrentSkipListSet<LockDescriptor> watches = new ConcurrentSkipListSet<>();

    public LockWatchingServiceImpl(LockEventLog lockEventLog, HeldLocksCollection heldLocksCollection) {
        this.lockEventLog = lockEventLog;
        this.heldLocksCollection = heldLocksCollection;
    }

    @Override
    public void startWatching(LockWatchRequest locksToWatch) {
        addToWatches(locksToWatch);
        logOpenLocks(locksToWatch);
        logLockWatchEvent(locksToWatch);
    }

    private void addToWatches(LockWatchRequest locksToWatch) {
        watches.addAll(locksToWatch.watchPrefixes());
    }

    private void logOpenLocks(LockWatchRequest locksToWatch) {
        lockEventLog.logOpenLocks(locksToWatch.watchPrefixes().stream()
                .map(heldLocksCollection::heldLocksMatching)
                .flatMap(Set::stream)
                .collect(Collectors.toSet()));
    }

    private void logLockWatchEvent(LockWatchRequest locksToWatch) {
        lockEventLog.logLockWatchCreated(locksToWatch);
    }

    @Override
    public void stopWatching(LockWatchRequest locksToUnwatch) {
        throw new UnsupportedOperationException("Not implemented in this version");
    }

    @Override
    public LockWatchStateUpdate getWatchState(Optional<Long> lastKnownVersion) {
        return lockEventLog.getLogDiff(lastKnownVersion);
    }

    @Override
    public void registerLock(Set<LockDescriptor> locksTakenOut, LockToken lockToken) {
        lockEventLog.logLock(locksTakenOut.stream().filter(this::hasLockWatch));
    }

    @Override
    public void registerUnlock(Set<LockDescriptor> unlocked) {
        lockEventLog.logUnlock(unlocked.stream().filter(this::hasLockWatch));
    }

    private boolean hasLockWatch(LockDescriptor lockDescriptor) {
        LockDescriptor candidate = watches.headSet(lockDescriptor, true).last();
        return candidate != null && candidate.isPrefixOf(lockDescriptor);
    }
}
