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
import com.palantir.lock.cache.AbstractLockWatchValueCache;
import com.palantir.lock.cache.NoOpValueCache;
import com.palantir.lock.watch.LockWatchEventCache;
import com.palantir.lock.watch.NoOpLockWatchEventCache;
import com.palantir.lock.watch.StartTransactionsLockWatchEventCache;
import java.util.Optional;
import org.immutables.value.Value;

public final class RequestBatchersFactory<T> {
    private final LockWatchEventCache lockWatchEventCache;
    private final AbstractLockWatchValueCache<T, ?> lockWatchValueCache;
    private final StartTransactionsLockWatchEventCache startTransactionsLockWatchEventCache;
    private final Namespace namespace;
    private final Optional<MultiClientRequestBatchers<T>> maybeRequestBatchers;

    private RequestBatchersFactory(
            LockWatchEventCache lockWatchEventCache,
            AbstractLockWatchValueCache<T, ?> lockWatchValueCache,
            Namespace namespace,
            Optional<MultiClientRequestBatchers<T>> maybeRequestBatchers) {
        this.lockWatchEventCache = lockWatchEventCache;
        this.startTransactionsLockWatchEventCache = StartTransactionsLockWatchEventCache.create(lockWatchEventCache);
        this.lockWatchValueCache = lockWatchValueCache;
        this.namespace = namespace;
        this.maybeRequestBatchers = maybeRequestBatchers;
    }

    public static <T> RequestBatchersFactory<T> create(
            LockWatchEventCache lockWatchEventCache,
            AbstractLockWatchValueCache<T, ?> lockWatchValueCache,
            Namespace namespace,
            Optional<MultiClientRequestBatchers<T>> maybeRequestBatchers) {
        return new RequestBatchersFactory<>(lockWatchEventCache, lockWatchValueCache, namespace, maybeRequestBatchers);
    }

    public static RequestBatchersFactory<Object> createForTests() {
        return new RequestBatchersFactory<>(
                NoOpLockWatchEventCache.create(),
                new NoOpValueCache<>(),
                Namespace.of("test-client"),
                Optional.empty());
    }

    public IdentifiedAtlasDbTransactionStarter createBatchingTransactionStarter(LockLeaseService lockLeaseService) {
        Optional<ReferenceTrackingWrapper<MultiClientTransactionStarter>> transactionStarter =
                maybeRequestBatchers.map(MultiClientRequestBatchers::transactionStarter);
        if (!transactionStarter.isPresent()) {
            return BatchingIdentifiedAtlasDbTransactionStarter.create(
                    lockLeaseService, startTransactionsLockWatchEventCache);
        }
        ReferenceTrackingWrapper<MultiClientTransactionStarter> referenceTrackingBatcher = transactionStarter.get();
        referenceTrackingBatcher.recordReference();
        return new NamespacedIdentifiedTransactionStarter(
                namespace,
                referenceTrackingBatcher,
                startTransactionsLockWatchEventCache,
                new LockCleanupService(lockLeaseService));
    }

    public CommitTimestampGetter<T> createBatchingCommitTimestampGetter(LockLeaseService lockLeaseService) {
        Optional<ReferenceTrackingWrapper<MultiClientCommitTimestampGetter<T>>> commitTimestampGetter =
                maybeRequestBatchers.map(MultiClientRequestBatchers::commitTimestampGetter);
        if (!commitTimestampGetter.isPresent()) {
            // todo(gmaretic): wire actual value cache from TMs.create()
            return BatchingCommitTimestampGetter.create(lockLeaseService, lockWatchEventCache, new NoOpValueCache<>());
        }
        ReferenceTrackingWrapper<MultiClientCommitTimestampGetter<T>> referenceTrackingBatcher =
                commitTimestampGetter.get();
        referenceTrackingBatcher.recordReference();
        return new NamespacedCommitTimestampGetter<T>(
                lockWatchEventCache, namespace, lockWatchValueCache, referenceTrackingBatcher);
    }

    @Value.Immutable
    public interface MultiClientRequestBatchers<T> {
        @Value.Parameter
        ReferenceTrackingWrapper<MultiClientCommitTimestampGetter<T>> commitTimestampGetter();

        @Value.Parameter
        ReferenceTrackingWrapper<MultiClientTransactionStarter> transactionStarter();
    }
}
