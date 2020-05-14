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

package com.palantir.lock.watch;

import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.palantir.logsafe.Preconditions;

public final class LockWatchEventCacheImpl implements LockWatchEventCache {
    private final ClientLockWatchEventLog lockWatchEventLog;
    // todo here - do we need a fancy cache, or just roll with a map (some concurrent form)?
    //  Need to decide how to make sure it doesn't grow unboundedly if we miss removing transactions
    //  (and for that matter - we don't have a way to remove transactions from this cache at the moment)
    private Cache<Long, Long> timestampCache = Caffeine.newBuilder()
            .expireAfterWrite(1, TimeUnit.HOURS) // Need to consider what happens if a transaction
            // exits without being cleared from the cache properly
            .build();

    public LockWatchEventCacheImpl(ClientLockWatchEventLog lockWatchEventLog) {
        this.lockWatchEventLog = lockWatchEventLog;
    }

    @Override
    public IdentifiedVersion lastKnownVersion() {
        return lockWatchEventLog.getLatestKnownVersion();
    }

    // todo - main thing here is to think about the concurrent accesses to this class
    @Override
    public IdentifiedVersion processStartTransactionsUpdate(Set<Long> startTimestamps, LockWatchStateUpdate update) {
        if (lockWatchEventLog.processUpdate(update)) {
            timestampCache.invalidateAll();
        } else {
            OptionalLong version = update.accept(new UpdateVisitor());
            if (version.isPresent()) {
                // Need to consider concurrent calls to this method
                startTimestamps.forEach(startTs -> timestampCache.put(startTs, version.getAsLong()));
            }
        }
        // This could have been updated in a bad order - need to perhaps take a snapshot at beginning of method
        return lastKnownVersion();
    }

    @Override
    public void processUpdate(LockWatchStateUpdate update) {
        lockWatchEventLog.processUpdate(update);
    }

    @Override
    public TransactionsLockWatchEvents getEventsForTransactions(Set<Long> startTimestamps, IdentifiedVersion version) {
        // If any are missing, we need to do something about it
        Map<Long, Long> timestampToVersion = timestampCache.getAllPresent(startTimestamps);
        Preconditions.checkState(timestampToVersion.size() == startTimestamps.size(),
                "Some timestamps are not in the cache");

        return lockWatchEventLog.getEventsForTransactions(timestampToVersion, version);
    }

    private static class UpdateVisitor implements LockWatchStateUpdate.Visitor<OptionalLong> {
        @Override
        public OptionalLong visit(LockWatchStateUpdate.Failed failed) {
            return OptionalLong.empty();
        }

        @Override
        public OptionalLong visit(LockWatchStateUpdate.Success success) {
            return OptionalLong.of(success.lastKnownVersion());
        }

        @Override
        public OptionalLong visit(LockWatchStateUpdate.Snapshot snapshot) {
            return OptionalLong.of(snapshot.lastKnownVersion());
        }
    }
}
