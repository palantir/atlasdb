/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsResponse;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.PartitionedTimestamps;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionResponse;
import com.palantir.lock.v2.TimestampAndPartition;
import com.palantir.lock.watch.LockWatchVersion;
import com.palantir.lock.watch.StartTransactionsLockWatchEventCache;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class TransactionStarterHelper {
    private TransactionStarterHelper() {
        // Do not instantiate helper class
    }

    static Set<LockToken> unlock(Set<LockToken> tokens, LockLeaseService lockLeaseService) {
        return unlock(tokens, lockLeaseService.lockCleanupService());
    }

    static Set<LockToken> unlock(Set<LockToken> tokens, LockCleanupService lockCleanupService) {
        Set<LockToken> lockTokens = filterOutTokenShares(tokens);

        Set<LockTokenShare> lockTokenShares = filterLockTokenShares(tokens);

        Set<LockToken> toUnlock = reduceForUnlock(lockTokenShares);
        Set<LockToken> toRefresh = getLockTokensToRefresh(lockTokenShares, toUnlock);

        Set<LockToken> refreshed = lockCleanupService.refreshLockLeases(toRefresh);
        Set<LockToken> unlocked = lockCleanupService.unlock(Sets.union(toUnlock, lockTokens));

        Set<LockTokenShare> resultLockTokenShares = Sets.filter(
                lockTokenShares,
                t -> unlocked.contains(t.sharedLockToken()) || refreshed.contains(t.sharedLockToken()));
        Set<LockToken> resultLockTokens = Sets.intersection(lockTokens, unlocked);

        return ImmutableSet.copyOf(Sets.union(resultLockTokenShares, resultLockTokens));
    }

    static Set<LockTokenShare> filterLockTokenShares(Set<LockToken> tokens) {
        return tokens.stream()
                .filter(TransactionStarterHelper::isLockTokenShare)
                .map(LockTokenShare.class::cast)
                .collect(Collectors.toSet());
    }

    static Set<LockToken> filterOutTokenShares(Set<LockToken> tokens) {
        return tokens.stream().filter(t -> !isLockTokenShare(t)).collect(Collectors.toSet());
    }

    /**
     * Calling unlock on a set of LockTokenShares only calls unlock on shared token iff all references to shared token
     * are unlocked.
     *
     * {@link com.palantir.lock.v2.TimelockService#unlock(Set)} has a guarantee that returned tokens were valid until
     * calling unlock. To keep that guarantee, we need to check if LockTokenShares were valid (by calling refresh with
     * referenced shared token) even if we don't unlock the underlying shared token.
     */
    private static Set<LockToken> getLockTokensToRefresh(
            Set<LockTokenShare> lockTokenShares, Set<LockToken> sharedTokensToUnlock) {
        return lockTokenShares.stream()
                .map(LockTokenShare::sharedLockToken)
                .filter(token -> !sharedTokensToUnlock.contains(token))
                .collect(Collectors.toSet());
    }

    private static Set<LockToken> reduceForUnlock(Set<LockTokenShare> lockTokenShares) {
        return lockTokenShares.stream()
                .map(LockTokenShare::unlock)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toSet());
    }

    private static boolean isLockTokenShare(LockToken lockToken) {
        return lockToken instanceof LockTokenShare;
    }

    static List<StartIdentifiedAtlasDbTransactionResponse> split(ConjureStartTransactionsResponse response) {
        PartitionedTimestamps partitionedTimestamps = response.getTimestamps();
        int partition = partitionedTimestamps.partition();

        LockToken immutableTsLock = response.getImmutableTimestamp().getLock();
        long immutableTs = response.getImmutableTimestamp().getImmutableTimestamp();

        Stream<LockImmutableTimestampResponse> immutableTsAndLocks = LockTokenShare.share(
                        immutableTsLock, partitionedTimestamps.count())
                .map(tokenShare -> LockImmutableTimestampResponse.of(immutableTs, tokenShare));

        Stream<TimestampAndPartition> timestampAndPartitions =
                partitionedTimestamps.stream().mapToObj(timestamp -> TimestampAndPartition.of(timestamp, partition));

        return Streams.zip(immutableTsAndLocks, timestampAndPartitions, StartIdentifiedAtlasDbTransactionResponse::of)
                .collect(Collectors.toList());
    }

    static void updateCacheWithStartTransactionResponse(
            StartTransactionsLockWatchEventCache lockWatchEventCache,
            Optional<LockWatchVersion> requestedVersion,
            ConjureStartTransactionsResponse response) {
        lockWatchEventCache.processStartTransactionsUpdate(
                response.getTimestamps().stream().boxed().collect(Collectors.toSet()), response.getLockWatchUpdate());
        LockWatchLogUtility.logTransactionEvents(requestedVersion, response.getLockWatchUpdate());
    }
}
