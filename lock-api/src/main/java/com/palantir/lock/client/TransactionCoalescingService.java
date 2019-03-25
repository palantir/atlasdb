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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher;
import com.palantir.common.base.Throwables;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.PartitionedTimestamps;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionResponse;
import com.palantir.lock.v2.StartTransactionResponseV4;
import com.palantir.lock.v2.TimestampAndPartition;

final class TransactionCoalescingService implements AutoCloseable {
    private final DisruptorAutobatcher<Void, StartIdentifiedAtlasDbTransactionResponse> autobatcher;
    private final LockLeaseService lockLeaseService;

    private TransactionCoalescingService(
            DisruptorAutobatcher<Void, StartIdentifiedAtlasDbTransactionResponse> autobatcher,
            LockLeaseService lockLeaseService) {
        this.autobatcher = autobatcher;
        this.lockLeaseService = lockLeaseService;
    }

    static TransactionCoalescingService create(LockLeaseService lockLeaseService) {
        return new TransactionCoalescingService(DisruptorAutobatcher.create(batch -> {
            int numTransactions = batch.size();

            List<StartIdentifiedAtlasDbTransactionResponse> startTransactionResponses =
                    getStartTransactionResponses(lockLeaseService, numTransactions);

            for (int i = 0; i < numTransactions; i++) {
                batch.get(i).result().set(startTransactionResponses.get(i));
            }
        }), lockLeaseService);
    }

    private static List<StartIdentifiedAtlasDbTransactionResponse> getStartTransactionResponses(
            LockLeaseService delegate, int numberOfTransactions) {
        List<StartIdentifiedAtlasDbTransactionResponse> result = new ArrayList<>();
        while (result.size() < numberOfTransactions) {
            result.addAll(split(delegate.startTransactions(numberOfTransactions - result.size())));
        }
        return result;
    }

    StartIdentifiedAtlasDbTransactionResponse startIdentifiedAtlasDbTransaction() {
        try {
            return autobatcher.apply(null).get();
        } catch (ExecutionException e) {
            throw Throwables.throwUncheckedException(e.getCause());
        } catch (Throwable t) {
            throw Throwables.throwUncheckedException(t);
        }
    }

    Set<LockToken> refreshLockLeases(Set<LockToken> tokens) {
        Set<LockTokenShare> lockTokenShares = filterLockTokenShares(tokens);
        Set<LockToken> lockTokens = filterOutTokenShares(tokens);

        Set<LockToken> refreshedTokens = lockLeaseService.refreshLockLeases(Sets.union(
                reduceForRefresh(lockTokenShares),
                lockTokens));

        Set<LockToken> resultLockTokenShares = lockTokenShares.stream()
                .filter(t -> refreshedTokens.contains(t.sharedLockToken()))
                .collect(Collectors.toSet());
        Set<LockToken> resultLockTokens = lockTokens.stream()
                .filter(refreshedTokens::contains)
                .collect(Collectors.toSet());

        return Sets.union(resultLockTokenShares, resultLockTokens);
    }

    Set<LockToken> unlock(Set<LockToken> tokens) {
        Set<LockToken> lockTokens = filterOutTokenShares(tokens);

        Set<LockTokenShare> lockTokenShares = filterLockTokenShares(tokens);
        Set<LockToken> referencedTokens = lockTokenShares.stream()
                .map(LockTokenShare::sharedLockToken)
                .collect(Collectors.toSet());

        Set<LockToken> toUnlock = reduceForUnlock(lockTokenShares);
        Set<LockToken> toRefresh = Sets.difference(referencedTokens, toUnlock);

        Set<LockToken> refreshed = refreshLockLeasesInternal(toRefresh);
        Set<LockToken> unlocked = lockLeaseService.unlock(Sets.union(toUnlock, lockTokens));

        Set<LockToken> resultLockTokenShares = lockTokenShares.stream()
                .filter(t -> unlocked.contains(t.sharedLockToken()) || refreshed.contains(t.sharedLockToken()))
                .collect(Collectors.toSet());
        Set<LockToken> resultLockTokens = lockTokens.stream().filter(unlocked::contains).collect(Collectors.toSet());

        return Sets.union(resultLockTokenShares, resultLockTokens);
    }

    @Override
    public void close() {
        autobatcher.close();
    }

    private Set<LockToken> refreshLockLeasesInternal(Set<LockToken> toRefresh) {
        if (!toRefresh.isEmpty()) {
            return lockLeaseService.refreshLockLeases(toRefresh);
        } else {
            return ImmutableSet.of();
        }
    }

    @VisibleForTesting
    static List<StartIdentifiedAtlasDbTransactionResponse> split(StartTransactionResponseV4 batchedResponse) {
        LockImmutableTimestampResponse immutableTsAndLock = batchedResponse.immutableTimestamp();
        PartitionedTimestamps partitionedTimestamps = batchedResponse.timestamps();
        int partition = partitionedTimestamps.partition();

        Stream<LockImmutableTimestampResponse> immutableTsAndLocks =
                LockTokenShare.share(immutableTsAndLock.getLock(), partitionedTimestamps.count()).stream()
                        .map(token ->
                                LockImmutableTimestampResponse.of(immutableTsAndLock.getImmutableTimestamp(), token));

        Stream<TimestampAndPartition> timestampAndPartitions = partitionedTimestamps.stream()
                .mapToObj(timestamp -> TimestampAndPartition.of(timestamp, partition));

        return Streams.zip(immutableTsAndLocks, timestampAndPartitions,
                StartIdentifiedAtlasDbTransactionResponse::of)
                .collect(Collectors.toList());
    }

    private static Set<LockToken> reduceForRefresh(Set<LockTokenShare> lockTokenShares) {
        return lockTokenShares.stream()
                .map(LockTokenShare::sharedLockToken)
                .collect(Collectors.toSet());
    }

    private static Set<LockToken> reduceForUnlock(Set<LockTokenShare> lockTokenShares) {
        return lockTokenShares.stream()
                .map(LockTokenShare::unlock)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toSet());
    }

    private static Set<LockTokenShare> filterLockTokenShares(Set<LockToken> tokens) {
        return tokens.stream().filter(t -> isLockTokenShare(t))
                .map(t -> (LockTokenShare) t)
                .collect(Collectors.toSet());
    }

    private static Set<LockToken> filterOutTokenShares(Set<LockToken> tokens) {
        return tokens.stream().filter(t -> !isLockTokenShare(t))
                .collect(Collectors.toSet());
    }


    private static boolean isLockTokenShare(LockToken lockToken) {
        return lockToken instanceof LockTokenShare;
    }
}
