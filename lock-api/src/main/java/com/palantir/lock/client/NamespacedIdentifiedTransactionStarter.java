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
import com.palantir.lock.watch.StartTransactionsLockWatchEventCache;
import java.util.List;

public class NamespacedIdentifiedTransactionStarter implements IdentifiedAtlasDbTransactionStarter {
    private final Namespace namespace;
    private final MultiClientTransactionStarter batcher;
    private final StartTransactionsLockWatchEventCache lockWatchEventCache;
    private final LockCleanupService lockCleanupService;

    public NamespacedIdentifiedTransactionStarter(
            Namespace namespace,
            MultiClientTransactionStarter batcher,
            StartTransactionsLockWatchEventCache lockWatchEventCache,
            LockCleanupService lockCleanupService) {
        this.namespace = namespace;
        this.batcher = batcher;
        this.lockWatchEventCache = lockWatchEventCache;
        this.lockCleanupService = lockCleanupService;
    }

    @Override
    public List<StartIdentifiedAtlasDbTransactionResponse> startIdentifiedAtlasDbTransactionBatch(int count) {
        return batcher.startTransactions(namespace, count, lockWatchEventCache, lockCleanupService);
    }

    @Override
    public void close() {
        batcher.close();
    }
}
