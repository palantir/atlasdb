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

import java.util.Collection;
import java.util.Optional;
import java.util.Set;

public interface LockWatchEventCache {

    // TODO: this needs to remember all requests, and make sure to re-register watches once it discovers a leader switch
    void registerLockWatches(Set<LockWatchReferences.LockWatchReference> lockWatchReferences);

    /**
     * Returns the last known lock watch version for the cache.
     */
    Optional<IdentifiedVersion> lastKnownVersion();

    /**
     * Updates the cache with the update, and identifies the given start timestamps with that lock watch state.
     */
    void processStartTransactionsUpdate(Set<Long> startTimestamps, LockWatchStateUpdate update);

    /**
     * Updates the cache with the update, and identifies the given commit timestamps with that lock watch state.
     */
    void processGetCommitTimestampsUpdate(Collection<TransactionUpdate> transactionUpdates,
            LockWatchStateUpdate update);

    /**
     * Updates the cache with the update, and calculates the {@link CommitUpdate} taking into account all changes to
     * lock watch state since the start of the transaction, excluding the transaction's own commit locks.
     *
     * @param startTs start timestamp of the transaction
     * @return the commit update for this transaction's precommit condition
     */
    CommitUpdate getCommitUpdate(long startTs);

    /**
     * Given a set of start timestamps, and a lock watch state version, returns a list of all events that occurred since
     * that version, and a map associating each start timestamp with its respective lock watch state version.
     */
    TransactionsLockWatchEvents getEventsForTransactions(Set<Long> startTimestamps,
            Optional<IdentifiedVersion> version);

    /**
     * This should clear all state, both initial mapping for IdentifiedVersion as well as any commit log information.
     *
     * @param startTimestamp
     */
    void removeTransactionStateFromCache(long startTimestamp);
}
