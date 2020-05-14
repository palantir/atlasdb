/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

import com.palantir.lock.client.NamespacedConjureLockWatchingService;
import com.palantir.lock.watch.IdentifiedVersion;
import com.palantir.lock.watch.LockWatchEventCache;
import com.palantir.lock.watch.LockWatchReferences;
import com.palantir.lock.watch.NoOpLockWatchEventCache;
import com.palantir.lock.watch.TransactionsLockWatchEvents;

public final class LockWatchServiceImpl implements LockWatchService {
    private final LockWatchManager lockWatchManager;
    private final LockWatchEventCache cache;

    private LockWatchServiceImpl(LockWatchManager lockWatchManager, LockWatchEventCache cache) {
        this.lockWatchManager = lockWatchManager;
        this.cache = cache;
    }

    public static LockWatchService create(
            NamespacedConjureLockWatchingService lockWatcher,
            LockWatchEventCache cache) {
        LockWatchManager lockWatchManager = new LockWatchManagerImpl(lockWatcher, cache);
        return new LockWatchServiceImpl(lockWatchManager, cache);
    }

    public static LockWatchService noWatches() {
        LockWatchManager lockWatchManager = NoOpLockWatchManager.INSTANCE;
        return new LockWatchServiceImpl(lockWatchManager, NoOpLockWatchEventCache.INSTANCE);
    }

    @Override
    public void registerWatches(Set<LockWatchReferences.LockWatchReference> lockWatchReferences) {
        lockWatchManager.registerWatches(lockWatchReferences);
    }

    @Override
    public TransactionsLockWatchEvents getEventsForTransactions(Set<Long> startTimestamps, IdentifiedVersion version) {
        return lockWatchManager.getEventsForTransactions(startTimestamps, version);
    }

    // This method is temporary - will be replaced by getting the commit update, which will remove the relevant
    // elements from the cache instead.
    @Override
    public void removeTimestampFromCache(long startTimestamp) {
        cache.removeTimestampFromCache(startTimestamp);
    }
}
