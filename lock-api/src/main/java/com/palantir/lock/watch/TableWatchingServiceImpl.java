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

package com.palantir.lock.watch;

import java.util.Set;
import java.util.stream.Collectors;

import com.palantir.lock.LockDescriptor;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.NamespacedTimelockRpcClient;

public class TableWatchingServiceImpl implements TableWatchingService {
    private final NamespacedLockWatchingRpcClient lockWatcher;
    private final NamespacedTimelockRpcClient timelock;
    private final LockWatchEventTracker tracker;

    public TableWatchingServiceImpl(NamespacedLockWatchingRpcClient lockWatcher,
            NamespacedTimelockRpcClient timelock,
            LockWatchEventTracker tracker) {
        this.lockWatcher = lockWatcher;
        this.timelock = timelock;
        this.tracker = tracker;
    }

    @Override
    public void registerWatches(Set<LockWatchReferences.LockWatchReference> lockWatchEntries) {
        lockWatcher.startWatching(LockWatchRequest.of(lockWatchEntries));
    }

    @Override
    public VersionedLockWatchState getLockWatchState(long startTimestamp) {
        return tracker.getLockWatchStateForStartTimestamp(startTimestamp);
    }

    @Override
    public TimestampWithLockInfo getCommitTimestampWithLockInfo(long startTimestamp, LockToken locksToIgnore) {
        VersionedLockWatchState startState = tracker.removeLockWatchStateForStartTimestamp(startTimestamp);
        TimestampWithWatches response = timelock.getCommitTimestampWithWatches(startState.version());
        LockWatchStateUpdate update = response.lockWatches();
        if (!update.success() || update.leaderId() != startState.leaderId()) {
            return TimestampWithLockInfo.invalidate(response.timestamp());
        }

        NewLocksVisitor visitor = NewLocksVisitor.ignoring(locksToIgnore);
        Set<LockDescriptor> lockedDescriptors = update.events().stream()
                .map(event -> event.accept(visitor))
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
        return TimestampWithLockInfo.diff(response.timestamp(), lockedDescriptors);
    }
}
