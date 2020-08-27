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

import com.palantir.common.annotation.Idempotent;
import com.palantir.lock.watch.CommitUpdate;
import com.palantir.lock.watch.IdentifiedVersion;
import com.palantir.lock.watch.LockWatchReferences;
import com.palantir.lock.watch.TransactionsLockWatchUpdate;
import java.util.Optional;
import java.util.Set;

public abstract class LockWatchManager {
    /**
     * Registers a set of lock watches.
     */
    @Idempotent
    public abstract void registerWatches(Set<LockWatchReferences.LockWatchReference> lockWatchReferences);

    // These methods are hidden on purpose as they should not be generally available, only for brave souls!

    /**
     * Gets the {@link CommitUpdate} taking into account all changes to lock watch state since the start of the
     * transaction, excluding the transaction's own commit locks.
     *
     * @param startTs start timestamp of the transaction
     * @return the commit update for this transaction
     */
    abstract CommitUpdate getCommitUpdate(long startTs);

    /**
     * Given a set of start timestamps, and a lock watch state version, returns a list of all events that occurred since
     * that version, and a map associating each start timestamp with its respective lock watch state version, and a flag
     * that is true if a snapshot or timelock leader election occurred.
     */
    abstract TransactionsLockWatchUpdate getUpdateForTransactions(Set<Long> startTimestamps,
            Optional<IdentifiedVersion> version);
}
