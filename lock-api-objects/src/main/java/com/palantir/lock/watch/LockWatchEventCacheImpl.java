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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class LockWatchEventCacheImpl implements LockWatchEventCache {
    private final ClientLockWatchEventLog lockWatchEventLog;
    private final Map<Long, Long> timestampMap = new ConcurrentHashMap<>();

    private LockWatchEventCacheImpl(ClientLockWatchEventLog lockWatchEventLog) {
        this.lockWatchEventLog = lockWatchEventLog;
    }

    public static LockWatchEventCacheImpl create() {
        return new LockWatchEventCacheImpl(NoOpLockWatchEventCache.INSTANCE);
    }

    @Override
    public IdentifiedVersion lastKnownVersion() {
        return lockWatchEventLog.getLatestKnownVersion();
    }

    @Override
    public IdentifiedVersion processStartTransactionsUpdate(Set<Long> startTimestamps, LockWatchStateUpdate update) {
        IdentifiedVersion currentVersion = lastKnownVersion();
        IdentifiedVersion newVersion = lockWatchEventLog.processUpdate(update);
        Optional<LockWatchStateUpdate.Success> successUpdate = update.accept(SuccessfulVisitor.INSTANCE);

        // clear for snapshot or failure, or leader change.
        if (!successUpdate.isPresent() || !currentVersion.id().equals(newVersion.id())) {
            timestampMap.clear();
        } else {
            long version = successUpdate.get().lastKnownVersion();
            startTimestamps.forEach(startTs -> timestampMap.put(startTs, version));
        }
        return newVersion;
    }

    @Override
    public void processUpdate(LockWatchStateUpdate update) {
        lockWatchEventLog.processUpdate(update);
    }

    @Override
    public TransactionsLockWatchEvents getEventsForTransactions(Set<Long> startTimestamps, IdentifiedVersion version) {
        Map<Long, Long> timestampToVersion = new HashMap<>();
        startTimestamps.forEach(startTs -> {
            Long value = timestampMap.get(startTs);
            // for now, skip entries not there.
            if (value != null) {
                timestampToVersion.put(startTs, value);
            }
        });
        return lockWatchEventLog.getEventsForTransactions(timestampToVersion, version);
    }

    @Override
    public void removeTimestampFromCache(Long timestamp) {
        timestampMap.remove(timestamp);
    }

    private enum SuccessfulVisitor implements LockWatchStateUpdate.Visitor<Optional<LockWatchStateUpdate.Success>> {
        INSTANCE;

        @Override
        public Optional<LockWatchStateUpdate.Success> visit(LockWatchStateUpdate.Failed failed) {
            return Optional.empty();
        }

        @Override
        public Optional<LockWatchStateUpdate.Success> visit(LockWatchStateUpdate.Success success) {
            return Optional.of(success);
        }

        @Override
        public Optional<LockWatchStateUpdate.Success> visit(LockWatchStateUpdate.Snapshot snapshot) {
            return Optional.empty();
        }
    }
}
