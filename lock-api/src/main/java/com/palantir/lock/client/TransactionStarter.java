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

package com.palantir.lock.client;

import com.google.common.collect.Sets;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionResponse;
import com.palantir.lock.watch.LockWatchEventCache;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A service responsible for coalescing multiple start transaction calls into a single start transactions call. This
 * service also handles creating {@link LockTokenShare}'s to enable multiple transactions sharing a single immutable
 * timestamp.
 *
 * Callers of this class should use {@link #unlock(Set)} and {@link #refreshLockLeases(Set)} for returned lock tokens,
 * rather than directly calling delegate lock service.
 */
class TransactionStarter implements AutoCloseable {
    private final LockLeaseService lockLeaseService;
    private final BatchingTransactionStarter batchingTransactionStarter;

    private TransactionStarter(LockLeaseService lockLeaseService, LockWatchEventCache lockWatchEventCache) {
        this.lockLeaseService = lockLeaseService;
        this.batchingTransactionStarter = BatchingTransactionStarter.create(lockLeaseService, lockWatchEventCache);
    }

    static TransactionStarter create(LockLeaseService lockLeaseService, LockWatchEventCache lockWatchEventCache) {
        return new TransactionStarter(lockLeaseService, lockWatchEventCache);
    }

    List<StartIdentifiedAtlasDbTransactionResponse> startIdentifiedAtlasDbTransactionBatch(int count) {
        return batchingTransactionStarter.startIdentifiedAtlasDbTransactionBatch(count);
    }

    Set<LockToken> refreshLockLeases(Set<LockToken> tokens) {
        Set<LockTokenShare> lockTokenShares = TransactionStarterHelper.filterLockTokenShares(tokens);
        Set<LockToken> lockTokens = TransactionStarterHelper.filterOutTokenShares(tokens);

        Set<LockToken> refreshedTokens = lockLeaseService.refreshLockLeases(
                Sets.union(TransactionStarterHelper.reduceForRefresh(lockTokenShares), lockTokens));

        Set<LockToken> resultLockTokenShares = lockTokenShares.stream()
                .filter(t -> refreshedTokens.contains(t.sharedLockToken()))
                .collect(Collectors.toSet());
        Set<LockToken> resultLockTokens =
                lockTokens.stream().filter(refreshedTokens::contains).collect(Collectors.toSet());

        return Sets.union(resultLockTokenShares, resultLockTokens);
    }

    Set<LockToken> unlock(Set<LockToken> tokens) {
        return TransactionStarterHelper.unlock(tokens, lockLeaseService);
    }

    @Override
    public void close() {
        batchingTransactionStarter.close();
    }
}
