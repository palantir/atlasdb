/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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
import com.palantir.lock.watch.LockWatchCache;
import com.palantir.lock.watch.LockWatchVersion;
import com.palantir.lock.watch.TransactionsLockWatchUpdate;
import java.util.Optional;
import java.util.Set;

public class ExposedLockWatchManager {
    private final LockWatchManagerInternal delegate;

    public ExposedLockWatchManager(LockWatchManager delegate) {
        this.delegate = (LockWatchManagerInternal) delegate;
    }

    public CommitUpdate getCommitUpdate(long startTs) {
        return delegate.getCommitUpdate(startTs);
    }

    public LockWatchCache getCache() {
        return delegate.getCache();
    }

    public TransactionsLockWatchUpdate getUpdateForTransactions(
            Set<Long> startTimestamps, Optional<LockWatchVersion> version) {
        return delegate.getUpdateForTransactions(startTimestamps, version);
    }
}
