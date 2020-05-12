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
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

public final class LockWatchEventCacheImpl implements LockWatchEventCache {
    private final ClientLockWatchEventLog lockWatchEventLog;
    private Cache<Long, Long> timestampCache = Caffeine.newBuilder()
            .expireAfterWrite(1, TimeUnit.HOURS) // this is a random time - we want some expiry but should be based
            // on a relevant constant
            .build();

    public LockWatchEventCacheImpl(ClientLockWatchEventLog lockWatchEventLog) {
        this.lockWatchEventLog = lockWatchEventLog;
    }

    @Override
    public IdentifiedVersion lastKnownVersion() {
        return lockWatchEventLog.getLatestKnownVersion();
    }

    @Override
    public IdentifiedVersion processStartTransactionsUpdate(Set<Long> startTimestamps, LockWatchStateUpdate update) {
        if (lockWatchEventLog.processUpdate(update)) {
            timestampCache.invalidateAll();
        } else {
            Long version = update.accept(new Visitor());
            if (version != null) {
                // this should fail midway through if we invalidate
                startTimestamps.forEach(startTs -> timestampCache.put(startTs, version));
            }
        }
        // todo - determine whether we need to return anything
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
        if(timestampToVersion.size() != startTimestamps.size()) {
            // do something bad
        }

        return lockWatchEventLog.getEventsForTransactions(timestampToVersion, version);
    }

    private static class Visitor implements LockWatchStateUpdate.Visitor<Long> {
        @Override
        public Long visit(LockWatchStateUpdate.Failed failed) {
            return null;
        }

        @Override
        public Long visit(LockWatchStateUpdate.Success success) {
            return success.lastKnownVersion();
        }

        @Override
        public Long visit(LockWatchStateUpdate.Snapshot snapshot) {
            return snapshot.lastKnownVersion();
        }
    }
}
