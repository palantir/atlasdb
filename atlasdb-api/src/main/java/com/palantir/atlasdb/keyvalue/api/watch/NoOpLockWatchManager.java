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

import com.palantir.lock.watch.CommitUpdate;
import com.palantir.lock.watch.IdentifiedVersion;
import com.palantir.lock.watch.LockWatchReferences;
import com.palantir.lock.watch.NoOpLockWatchEventCache;
import com.palantir.lock.watch.TransactionsLockWatchUpdate;
import java.util.Optional;
import java.util.Set;

public final class NoOpLockWatchManager extends LockWatchManager {
    public static final LockWatchManager INSTANCE = new NoOpLockWatchManager();

    @Override
    public void registerWatches(Set<LockWatchReferences.LockWatchReference> lockWatchReferences) {
        // Ignored
    }

    @Override
    CommitUpdate getCommitUpdate(long startTs) {
        return NoOpLockWatchEventCache.INSTANCE.getCommitUpdate(startTs);
    }

    @Override
    TransactionsLockWatchUpdate getUpdateForTransactions(Set<Long> startTimestamps,
            Optional<IdentifiedVersion> version) {
        return NoOpLockWatchEventCache.INSTANCE.getUpdateForTransactions(startTimestamps, version);
    }
}
