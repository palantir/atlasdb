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
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionResponse;
import com.palantir.lock.watch.LockWatchCache;
import java.util.List;

public class NamespacedIdentifiedTransactionStarter implements IdentifiedAtlasDbTransactionStarter {
    private final Namespace namespace;
    private final ReferenceTrackingWrapper<MultiClientTransactionStarter> referenceTrackingBatcher;
    private final LockWatchCache cache;
    private final LockCleanupService lockCleanupService;

    public NamespacedIdentifiedTransactionStarter(
            Namespace namespace,
            ReferenceTrackingWrapper<MultiClientTransactionStarter> referenceTrackingBatcher,
            LockWatchCache cache,
            LockCleanupService lockCleanupService) {
        this.namespace = namespace;
        this.referenceTrackingBatcher = referenceTrackingBatcher;
        this.cache = cache;
        this.lockCleanupService = lockCleanupService;
    }

    @Override
    public List<StartIdentifiedAtlasDbTransactionResponse> startIdentifiedAtlasDbTransactionBatch(int count) {
        return referenceTrackingBatcher.getDelegate().startTransactions(namespace, count, cache, lockCleanupService);
    }

    @Override
    public void close() {
        referenceTrackingBatcher.close();
    }
}
