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
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import com.palantir.atlasdb.autobatch.Autobatchers;
import com.palantir.atlasdb.autobatch.BatchElement;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher;
import com.palantir.common.base.Throwables;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.PartitionedTimestamps;
import com.palantir.lock.v2.StartTransactionResponseV5;
import com.palantir.lock.v2.StartTransactionWithWatchesResponse;
import com.palantir.lock.v2.TimestampAndPartition;
import com.palantir.lock.watch.LockWatchEventLog;
import com.palantir.lock.watch.VersionedLockWatchState;

/**
 * A service responsible for coalescing multiple start transaction calls into a single start transactions call. This
 * service also handles creating {@link LockTokenShare}'s to enable multiple transactions sharing a single immutable
 * timestamp.
 *
 * Callers of this class should use {@link #unlock(Set)} and {@link #refreshLockLeases(Set)} for returned lock tokens,
 * rather than directly calling delegate lock service.
 */
final class TransactionStarter implements AutoCloseable {
    private final DisruptorAutobatcher<Void, StartTransactionWithWatchesResponse> autobatcher;
    private final LockLeaseService lockLeaseService;

    private TransactionStarter(
            DisruptorAutobatcher<Void, StartTransactionWithWatchesResponse> autobatcher,
            LockLeaseService lockLeaseService) {
        this.autobatcher = autobatcher;
        this.lockLeaseService = lockLeaseService;
    }

    static TransactionStarter create(LockLeaseService lockLeaseService, LockWatchEventLog lockWatchLog) {
        DisruptorAutobatcher<Void, StartTransactionWithWatchesResponse> autobatcher = Autobatchers
                .independent(consumer(lockLeaseService, lockWatchLog))
                .safeLoggablePurpose("transaction-starter")
                .build();
        return new TransactionStarter(autobatcher,
                lockLeaseService);
    }

    StartTransactionWithWatchesResponse startIdentifiedAtlasDbTransaction() {
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

        Set<LockToken> toUnlock = reduceForUnlock(lockTokenShares);
        Set<LockToken> toRefresh = getLockTokensToRefresh(lockTokenShares, toUnlock);

        Set<LockToken> refreshed = lockLeaseService.refreshLockLeases(toRefresh);
        Set<LockToken> unlocked = lockLeaseService.unlock(Sets.union(toUnlock, lockTokens));

        Set<LockTokenShare> resultLockTokenShares = Sets.filter(
                lockTokenShares,
                t -> unlocked.contains(t.sharedLockToken()) || refreshed.contains(t.sharedLockToken()));
        Set<LockToken> resultLockTokens = Sets.intersection(lockTokens, unlocked);

        return ImmutableSet.copyOf(Sets.union(resultLockTokenShares, resultLockTokens));
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

    @Override
    public void close() {
        autobatcher.close();
    }

    @VisibleForTesting
    static Consumer<List<BatchElement<Void, StartTransactionWithWatchesResponse>>> consumer(
            LockLeaseService lockLeaseService, LockWatchEventLog lockWatchLog) {
        return batch -> {
            int numTransactions = batch.size();

            List<StartTransactionWithWatchesResponse> startTransactionResponses =
                    getStartTransactionResponses(lockLeaseService, lockWatchLog, numTransactions);

            for (int i = 0; i < numTransactions; i++) {
                batch.get(i).result().set(startTransactionResponses.get(i));
            }
        };
    }

    private static List<StartTransactionWithWatchesResponse> getStartTransactionResponses(
            LockLeaseService lockLeaseService, LockWatchEventLog lockWatchLog, int numberOfTransactions) {
        List<StartTransactionWithWatchesResponse> result = new ArrayList<>();
        while (result.size() < numberOfTransactions) {
            StartTransactionResponseV5 response = lockLeaseService.startTransactionsWithWatches(
                    lockWatchLog.currentState().version(), numberOfTransactions - result.size());
            VersionedLockWatchState lockWatchState = lockWatchLog.updateState(response.lockWatchUpdate());
            result.addAll(split(response, lockWatchState));
        }
        return result;
    }

    private static List<StartTransactionWithWatchesResponse> split(StartTransactionResponseV5 batchedResponse,
            VersionedLockWatchState lockWatchState) {
        PartitionedTimestamps partitionedTimestamps = batchedResponse.timestamps();
        int partition = partitionedTimestamps.partition();

        LockToken immutableTsLock = batchedResponse.immutableTimestamp().getLock();
        long immutableTs = batchedResponse.immutableTimestamp().getImmutableTimestamp();

        Stream<LockImmutableTimestampResponse> immutableTsAndLocks =
                LockTokenShare.share(immutableTsLock, partitionedTimestamps.count())
                        .map(tokenShare -> LockImmutableTimestampResponse.of(
                                immutableTs,
                                tokenShare));

        Stream<TimestampAndPartition> timestampAndPartitions = partitionedTimestamps.stream()
                .mapToObj(timestamp -> TimestampAndPartition.of(timestamp, partition));

        return Streams.zip(immutableTsAndLocks, timestampAndPartitions,
                (x, y) -> StartTransactionWithWatchesResponse.of(x, y, lockWatchState))
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
        return tokens.stream().filter(TransactionStarter::isLockTokenShare)
                .map(LockTokenShare.class::cast)
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
