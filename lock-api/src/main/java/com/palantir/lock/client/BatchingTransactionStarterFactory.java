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
import com.palantir.lock.watch.StartTransactionsLockWatchEventCache;
import java.util.Optional;

public final class BatchingTransactionStarterFactory {
    private final Optional<MultiClientTransactionStarter> maybeBatcher;
    private final Optional<Namespace> namespace;
    private final StartTransactionsLockWatchEventCache cache;

    public BatchingTransactionStarterFactory(
            StartTransactionsLockWatchEventCache lockWatchEventCache,
            Optional<Namespace> namespace,
            Optional<MultiClientTransactionStarter> maybeBatcher) {
        this.maybeBatcher = maybeBatcher;
        this.namespace = namespace;
        this.cache = lockWatchEventCache;
    }

    public IdentifiedAtlasDbTransactionStarter get(LockLeaseService lockLeaseService) {
        if (!namespace.isPresent() || !maybeBatcher.isPresent()) {
            return BatchingIdentifiedAtlasDbTransactionStarter.create(lockLeaseService, cache);
        }
        return new NamespacedIdentifiedTransactionStarter(
                namespace.get(), maybeBatcher.get(), cache, new LockCleanupService(lockLeaseService));
    }
}
