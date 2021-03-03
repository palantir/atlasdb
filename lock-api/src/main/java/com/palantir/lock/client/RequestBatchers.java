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

package com.palantir.lock.client;

import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.lock.watch.LockWatchEventCache;
import com.palantir.lock.watch.NoOpLockWatchEventCache;
import com.palantir.lock.watch.StartTransactionsLockWatchEventCache;
import java.util.Optional;

public final class RequestBatchers {
    private final LockWatchEventCache lockWatchEventCache;
    private final StartTransactionsLockWatchEventCache startTransactionsLockWatchEventCache;
    private final Optional<Namespace> namespace;
    private final Optional<MultiClientTransactionStarter> maybeTransactionStarter;
    private final Optional<MultiClientCommitTimestampGetter> maybeCommitTimestampGetter;

    private RequestBatchers(
            LockWatchEventCache lockWatchEventCache,
            Optional<Namespace> namespace,
            Optional<MultiClientTransactionStarter> maybeTransactionStarter,
            Optional<MultiClientCommitTimestampGetter> maybeCommitTimestampGetter) {
        this.lockWatchEventCache = lockWatchEventCache;
        this.startTransactionsLockWatchEventCache = StartTransactionsLockWatchEventCache.create(lockWatchEventCache);
        this.namespace = namespace;
        this.maybeTransactionStarter = maybeTransactionStarter;
        this.maybeCommitTimestampGetter = maybeCommitTimestampGetter;
    }

    public static RequestBatchers create(
            LockWatchEventCache lockWatchEventCache,
            Optional<Namespace> namespace,
            Optional<MultiClientTransactionStarter> maybeBatcher,
            Optional<MultiClientCommitTimestampGetter> maybeCommitTimestampGetter) {
        return new RequestBatchers(lockWatchEventCache, namespace, maybeBatcher, maybeCommitTimestampGetter);
    }

    public static RequestBatchers createForTests() {
        return new RequestBatchers(
                NoOpLockWatchEventCache.create(), Optional.empty(), Optional.empty(), Optional.empty());
    }

    public IdentifiedAtlasDbTransactionStarter getBatchingTransactionStarter(LockLeaseService lockLeaseService) {
        if (!namespace.isPresent() || !maybeTransactionStarter.isPresent()) {
            return BatchingIdentifiedAtlasDbTransactionStarter.create(
                    lockLeaseService, startTransactionsLockWatchEventCache);
        }
        return new NamespacedIdentifiedTransactionStarter(
                namespace.get(),
                maybeTransactionStarter.get(),
                startTransactionsLockWatchEventCache,
                new LockCleanupService(lockLeaseService));
    }

    public CommitTimestampGetter getBatchingCommitTimestampGetter(LockLeaseService lockLeaseService) {
        if (!namespace.isPresent() || !maybeCommitTimestampGetter.isPresent()) {
            return BatchingCommitTimestampGetter.create(lockLeaseService, lockWatchEventCache);
        }
        return new NamespacedCommitTimestampGetter(
                lockWatchEventCache, namespace.get(), maybeCommitTimestampGetter.get());
    }
}
