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

package com.palantir.atlasdb.transaction.api;

import java.util.OptionalLong;
import java.util.Set;

import com.palantir.lock.LockDescriptor;
import com.palantir.lock.watch.LockWatchEventLog;
import com.palantir.lock.watch.LockWatchRequest;
import com.palantir.lock.watch.LockWatchStateUpdate;
import com.palantir.lock.watch.NamespacedLockWatchingRpcClient;
import com.palantir.lock.watch.NewLocksVisitor;
import com.palantir.lock.watch.VersionedLockWatchState;

public class KvsLockWatchingServiceImpl implements KvsLockWatchingService {
    private final NamespacedLockWatchingRpcClient client;
    private final LockWatchEventLog lockWatchEventLog;

    public KvsLockWatchingServiceImpl(NamespacedLockWatchingRpcClient client,
            LockWatchEventLog lockWatchEventLog) {
        this.client = client;
        this.lockWatchEventLog = lockWatchEventLog;
    }

    @Override
    public void registerWatches(LockWatchRequest lockWatchEntries) {
        client.startWatching(lockWatchEntries);
    }

    @Override
    public VersionedLockWatchState getLockWatchState() {
        return lockWatchEventLog.currentState();
    }

    @Override
    public Set<LockDescriptor> lockedSinceVersion(long version) {
        LockWatchStateUpdate update = client.getWatchStateUpdate(OptionalLong.of(version));
        lockWatchEventLog.updateState(update);
        NewLocksVisitor visitor = new NewLocksVisitor();
        update.events().forEach(event -> event.accept(visitor));
        return visitor.getLocks();
    }
}
