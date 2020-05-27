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

import java.util.Optional;
import java.util.Set;

import com.palantir.atlasdb.timelock.api.LockWatchRequest;
import com.palantir.lock.client.NamespacedConjureLockWatchingService;
import com.palantir.lock.watch.IdentifiedVersion;
import com.palantir.lock.watch.LockWatchEventCache;
import com.palantir.lock.watch.LockWatchReferences;
import com.palantir.lock.watch.TransactionsLockWatchEvents;

public final class InternalLockWatchManagerImpl implements InternalLockWatchManager {
    private final NamespacedConjureLockWatchingService lockWatcher;
    private final LockWatchEventCache cache;

    public InternalLockWatchManagerImpl(NamespacedConjureLockWatchingService lockWatcher, LockWatchEventCache cache) {
        this.lockWatcher = lockWatcher;
        this.cache = cache;
    }

    @Override
    public void registerWatches(Set<LockWatchReferences.LockWatchReference> lockWatchEntries) {
        lockWatcher.startWatching(LockWatchRequest.of(lockWatchEntries));
    }

    @Override
    public TransactionsLockWatchEvents getEventsForTransactions(Set<Long> startTimestamps,
            Optional<IdentifiedVersion> version) {
        return cache.getEventsForTransactions(startTimestamps, version);
    }
}
