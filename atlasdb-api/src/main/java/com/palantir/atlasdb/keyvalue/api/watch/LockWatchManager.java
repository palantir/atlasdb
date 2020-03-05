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
import java.util.UUID;

import com.palantir.common.annotation.Idempotent;
import com.palantir.lock.watch.CommitUpdate;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.IdentifiedVersion;
import com.palantir.lock.watch.LockWatchReferences;
import com.palantir.lock.watch.TransactionsLockWatchEvents;

public interface LockWatchManager {
    /**
     * Registers a set of lock watches.
     */
    @Idempotent
    void registerWatches(Set<LockWatchReferences.LockWatchReference> lockWatchReferences);

    /**
     * Returns a condensed view of new lock watch events since lastKnownVersion for a set of transactions identified by
     * their start timestamps.
     * @param startTimestamps a set of start timestamps identifying transactions
     * @param lastKnownVersion exclusive start version to get events from
     */
    TransactionsLockWatchEvents getEventsForTransactions(Set<Long> startTimestamps, IdentifiedVersion lastKnownVersion);

    CommitUpdate getCommitUpdate(long startTimestamp, UUID requestIdToExclude);
}
