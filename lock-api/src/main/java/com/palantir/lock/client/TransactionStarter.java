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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import com.palantir.atlasdb.autobatch.Autobatchers;
import com.palantir.atlasdb.autobatch.BatchElement;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsResponse;
import com.palantir.common.base.Throwables;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.PartitionedTimestamps;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionResponse;
import com.palantir.lock.v2.TimestampAndPartition;
import com.palantir.lock.watch.LockWatchEventCache;
import com.palantir.logsafe.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A service responsible for coalescing multiple start transaction calls into a single start transactions call. This
 * service also handles creating {@link LockTokenShare}'s to enable multiple transactions sharing a single immutable
 * timestamp.
 *
 * Callers of this class should use {@link #unlock(Set)} and {@link #refreshLockLeases(Set)} for returned lock tokens,
 * rather than directly calling delegate lock service.
 */
final class TransactionStarter implements AutoCloseable {
    private final DisruptorAutobatcher<Integer, List<StartIdentifiedAtlasDbTransactionResponse>> autobatcher;
    private final LockLeaseService lockLeaseService;

    private TransactionStarter(
            DisruptorAutobatcher<Integer, List<StartIdentifiedAtlasDbTransactionResponse>> autobatcher,
            LockLeaseService lockLeaseService) {
        this.autobatcher = autobatcher;
        this.lockLeaseService = lockLeaseService;
    }

    static TransactionStarter create(LockLeaseService lockLeaseService, LockWatchEventCache lockWatchEventCache) {
        DisruptorAutobatcher<Integer, List<StartIdentifiedAtlasDbTransactionResponse>> autobatcher = Autobatchers
                .independent(consumer(lockLeaseService, lockWatchEventCache))
                .safeLoggablePurpose("transaction-starter")
                .build();
        return new TransactionStarter(autobatcher, lockLeaseService);
    }

    List<StartIdentifiedAtlasDbTransactionResponse> startIdentifiedAtlasDbTransactionBatch(int count) {
        Preconditions.checkArgument(count > 0, "Cannot start 0 or fewer transactions");
        return AtlasFutures.getUnchecked(autobatcher.apply(count));
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
        return unlock(tokens, lockLeaseService);
    }

    private static Set<LockToken> unlock(Set<LockToken> tokens, LockLeaseService lockLeaseService) {
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
    static Consumer<List<BatchElement<Integer, List<StartIdentifiedAtlasDbTransactionResponse>>>> consumer(
            LockLeaseService lockLeaseService, LockWatchEventCache lockWatchEventCache) {
        return batch -> {
            int numTransactions = batch.stream().mapToInt(BatchElement::argument).reduce(0, Integer::sum);

            List<StartIdentifiedAtlasDbTransactionResponse> startTransactionResponses =
                    getStartTransactionResponses(lockLeaseService, lockWatchEventCache, numTransactions);

            int start = 0;
            for (BatchElement<Integer, List<StartIdentifiedAtlasDbTransactionResponse>> batchElement
                    : batch) {
                int end = start + batchElement.argument();
                batchElement.result().set(ImmutableList.copyOf(startTransactionResponses.subList(start, end)));
                start = end;
            }
        };
    }

    private static List<StartIdentifiedAtlasDbTransactionResponse> getStartTransactionResponses(
            LockLeaseService lockLeaseService,
            LockWatchEventCache lockWatchEventCache,
            int numberOfTransactions) {
        List<StartIdentifiedAtlasDbTransactionResponse> result = new ArrayList<>();
        while (result.size() < numberOfTransactions) {
            try {
                ConjureStartTransactionsResponse response = lockLeaseService.startTransactionsWithWatches(
                        lockWatchEventCache.lastKnownVersion(), numberOfTransactions - result.size());
                lockWatchEventCache.processStartTransactionsUpdate(
                        response.getTimestamps().stream().boxed().collect(Collectors.toSet()),
                        response.getLockWatchUpdate());
                result.addAll(split(response));
            } catch (Throwable t) {
                unlock(result.stream()
                        .map(response -> response.immutableTimestamp().getLock())
                        .collect(Collectors.toSet()),
                        lockLeaseService);
                throw Throwables.throwUncheckedException(t);
            }
        }
        return result;
    }

    private static List<StartIdentifiedAtlasDbTransactionResponse> split(ConjureStartTransactionsResponse response) {
        PartitionedTimestamps partitionedTimestamps = response.getTimestamps();
        int partition = partitionedTimestamps.partition();

        LockToken immutableTsLock = response.getImmutableTimestamp().getLock();
        long immutableTs = response.getImmutableTimestamp().getImmutableTimestamp();

        Stream<LockImmutableTimestampResponse> immutableTsAndLocks =
                LockTokenShare.share(immutableTsLock, partitionedTimestamps.count())
                        .map(tokenShare -> LockImmutableTimestampResponse.of(
                                immutableTs,
                                tokenShare));

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
