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
import com.palantir.lock.watch.LockWatchCache;
import com.palantir.lock.watch.LockWatchCacheImpl;
import java.util.Optional;
import org.immutables.value.Value;

public final class RequestBatchersFactory {
    private final LockWatchCache cache;
    private final Namespace namespace;
    private final Optional<MultiClientRequestBatchers> maybeRequestBatchers;

    private RequestBatchersFactory(
            LockWatchCache cache, Namespace namespace, Optional<MultiClientRequestBatchers> maybeRequestBatchers) {
        this.cache = cache;
        this.namespace = namespace;
        this.maybeRequestBatchers = maybeRequestBatchers;
    }

    public static RequestBatchersFactory create(
            LockWatchCache cache, Namespace namespace, Optional<MultiClientRequestBatchers> maybeRequestBatchers) {
        return new RequestBatchersFactory(cache, namespace, maybeRequestBatchers);
    }

    public static RequestBatchersFactory createForTests() {
        return new RequestBatchersFactory(LockWatchCacheImpl.noop(), Namespace.of("test-client"), Optional.empty());
    }

    public IdentifiedAtlasDbTransactionStarter createBatchingTransactionStarter(LockLeaseService lockLeaseService) {
        Optional<ReferenceTrackingWrapper<MultiClientTransactionStarter>> transactionStarter =
                maybeRequestBatchers.map(MultiClientRequestBatchers::transactionStarter);
        if (!transactionStarter.isPresent()) {
            return BatchingIdentifiedAtlasDbTransactionStarter.create(lockLeaseService, cache);
        }
        ReferenceTrackingWrapper<MultiClientTransactionStarter> referenceTrackingBatcher = transactionStarter.get();
        referenceTrackingBatcher.recordReference();
        return new NamespacedIdentifiedTransactionStarter(
                namespace, referenceTrackingBatcher, cache, new LockCleanupService(lockLeaseService));
    }

    public CommitTimestampGetter createBatchingCommitTimestampGetter(LockLeaseService lockLeaseService) {
        Optional<ReferenceTrackingWrapper<MultiClientCommitTimestampGetter>> commitTimestampGetter =
                maybeRequestBatchers.map(MultiClientRequestBatchers::commitTimestampGetter);
        if (!commitTimestampGetter.isPresent()) {
            return BatchingCommitTimestampGetter.create(lockLeaseService, cache);
        }
        ReferenceTrackingWrapper<MultiClientCommitTimestampGetter> referenceTrackingBatcher =
                commitTimestampGetter.get();
        referenceTrackingBatcher.recordReference();
        return new NamespacedCommitTimestampGetter(cache, namespace, referenceTrackingBatcher);
    }

    @Value.Immutable
    public interface MultiClientRequestBatchers {
        @Value.Parameter
        ReferenceTrackingWrapper<MultiClientCommitTimestampGetter> commitTimestampGetter();

        @Value.Parameter
        ReferenceTrackingWrapper<MultiClientTransactionStarter> transactionStarter();
    }
}
