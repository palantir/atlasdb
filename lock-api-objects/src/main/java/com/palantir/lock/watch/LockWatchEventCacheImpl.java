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

public final class LockWatchEventCacheImpl implements LockWatchEventCache {
    private final ClientLockWatchEventLog lockWatchEventLog;

    public LockWatchEventCacheImpl(
            ClientLockWatchEventLog lockWatchEventLog) {
        this.lockWatchEventLog = lockWatchEventLog;
    }

    @Override
    public IdentifiedVersion lastKnownVersion() {
        return lockWatchEventLog.getLatestKnownVersion();
    }

    @Override
    public IdentifiedVersion processStartTransactionsUpdate(Set<Long> startTimestamps, LockWatchStateUpdate update) {
        // todo - do some processing to store the timestamps
        // note that we will need to also know the leader, lest it change and we also need to invalidate all
        processUpdate(update);
        // todo - determine whether we need to return anything
        return lastKnownVersion();
    }

    @Override
    public void processUpdate(LockWatchStateUpdate update) {
        lockWatchEventLog.processUpdate(update);
    }

    @Override
    public TransactionsLockWatchEvents getEventsForTransactions(Set<Long> startTimestamps, IdentifiedVersion version) {
        // todo - implement the piece where we cache timestamp -> version mappings
        return lockWatchEventLog.getEventsForTransactions(null, version);
    }
}
