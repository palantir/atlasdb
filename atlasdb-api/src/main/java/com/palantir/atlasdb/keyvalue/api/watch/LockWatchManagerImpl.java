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

import com.palantir.atlasdb.timelock.api.GetCommitTimestampsRequest;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsResponse;
import com.palantir.lock.client.NamespacedConjureTimelockService;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.CommitUpdate;
import com.palantir.lock.watch.IdentifiedVersion;
import com.palantir.lock.watch.LockWatchEventCache;
import com.palantir.lock.watch.LockWatchReferences;
import com.palantir.lock.watch.LockWatchRequest;
import com.palantir.lock.watch.NamespacedLockWatchingRpcClient;
import com.palantir.lock.watch.TransactionsLockWatchEvents;

public final class LockWatchManagerImpl implements LockWatchManager {
    private final NamespacedLockWatchingRpcClient lockWatchingRpcClient;
    private final NamespacedConjureTimelockService timelock;
    private final LockWatchEventCache cache;

    public LockWatchManagerImpl(
            NamespacedLockWatchingRpcClient lockWatchingRpcClient,
            NamespacedConjureTimelockService timelock,
            LockWatchEventCache cache) {
        this.lockWatchingRpcClient = lockWatchingRpcClient;
        this.timelock = timelock;
        this.cache = cache;
    }

    @Override
    public void registerWatches(Set<LockWatchReferences.LockWatchReference> lockWatchEntries) {
        lockWatchingRpcClient.startWatching(LockWatchRequest.of(lockWatchEntries));
    }

    @Override
    public TransactionsLockWatchEvents getEventsForTransactions(Set<Long> startTimestamps, IdentifiedVersion version) {
        return cache.getEventsForTransactions(startTimestamps, version);
    }

    @Override
    public CommitUpdate getCommitUpdate(long startTimestamp, LockToken commitLocksToken) {
        GetCommitTimestampsRequest request = GetCommitTimestampsRequest.builder()
                .numTimestamps(1)
                .lastKnownVersion(cache.lastKnownVersion().version())
                .build();
        GetCommitTimestampsResponse response = timelock.getCommitTimestamps(request);
        return cache.getCommitUpdate(
                startTimestamp,
                response.getInclusiveLower(),
                response.getLockWatchUpdate(),
                commitLocksToken);
    }
}
