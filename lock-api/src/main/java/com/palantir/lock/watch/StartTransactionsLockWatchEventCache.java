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

package com.palantir.lock.watch;

import com.palantir.lock.client.MultiClientTransactionStarter;
import java.util.Optional;
import java.util.Set;

/**
 * This is a facade of {@link LockWatchEventCache} meant to expose only the functionality required for starting a
 * batch of transactions, see {@link MultiClientTransactionStarter}.
 * */
public final class StartTransactionsLockWatchEventCache {
    private final LockWatchEventCache delegate;

    private StartTransactionsLockWatchEventCache(LockWatchEventCache delegate) {
        this.delegate = delegate;
    }

    public static StartTransactionsLockWatchEventCache create(LockWatchEventCache delegate) {
        return new StartTransactionsLockWatchEventCache(delegate);
    }

    public static StartTransactionsLockWatchEventCache createForTests() {
        return new StartTransactionsLockWatchEventCache(NoOpLockWatchEventCache.create());
    }

    public Optional<LockWatchVersion> lastKnownVersion() {
        return delegate.lastKnownVersion();
    }

    public void processStartTransactionsUpdate(Set<Long> startTimestamps, LockWatchStateUpdate update) {
        delegate.processStartTransactionsUpdate(startTimestamps, update);
    }
}
