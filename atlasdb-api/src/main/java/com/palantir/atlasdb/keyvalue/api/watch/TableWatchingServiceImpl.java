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

package com.palantir.atlasdb.keyvalue.api.watch;

import java.util.Set;
import java.util.stream.Collectors;

import com.palantir.lock.LockDescriptor;
import com.palantir.lock.v2.NamespacedTimelockRpcClient;
import com.palantir.lock.watch.LockWatchReferences;
import com.palantir.lock.watch.LockWatchRequest;
import com.palantir.lock.watch.LockWatchStateUpdate;
import com.palantir.lock.watch.NamespacedLockWatchingRpcClient;
import com.palantir.lock.watch.NewLocksVisitor;
import com.palantir.lock.watch.TimestampWithLockInfo;
import com.palantir.lock.watch.TimestampWithWatches;
import com.palantir.lock.watch.VersionedLockWatchState;

public class TableWatchingServiceImpl implements TableWatchingService {
    private final NamespacedLockWatchingRpcClient lockWatcher;
    private final NamespacedTimelockRpcClient timelock;

    public TableWatchingServiceImpl(NamespacedLockWatchingRpcClient lockWatcher, NamespacedTimelockRpcClient timelock) {
        this.lockWatcher = lockWatcher;
        this.timelock = timelock;
    }

    @Override
    public void registerWatches(Set<LockWatchReferences.LockWatchReference> lockWatchEntries) {
        lockWatcher.startWatching(LockWatchRequest.of(lockWatchEntries));
    }

    @Override
    public TimestampWithLockInfo getCommitTimestampWithLockInfo(VersionedLockWatchState oldState) {
        TimestampWithWatches response = timelock.getCommitTimestampWithWatches(oldState.version());
        LockWatchStateUpdate update = response.lockWatches();
        if (!update.success() || update.leaderId() != oldState.leaderId()) {
            return TimestampWithLockInfo.invalidate(response.timestamp());
        }

        Set<LockDescriptor> lockedDescriptors = update.events().stream()
                .map(event -> event.accept(NewLocksVisitor.INSTANCE))
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
        return TimestampWithLockInfo.diff(response.timestamp(), lockedDescriptors);
    }
}
