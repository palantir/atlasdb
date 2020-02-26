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

import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsRequest;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsResponse;
import com.palantir.common.concurrent.CoalescingSupplier;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.lock.v2.Lease;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockResponseV2;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.NamespacedTimelockRpcClient;
import com.palantir.lock.v2.RefreshLockResponseV2;
import com.palantir.lock.v2.StartTransactionRequestV4;
import com.palantir.lock.v2.StartTransactionRequestV5;
import com.palantir.lock.v2.StartTransactionResponseV4;
import com.palantir.lock.v2.StartTransactionResponseV5;
import com.palantir.logsafe.Preconditions;

class LockLeaseService {
    private static final boolean HAVE_ROLLED_OUT_CONJURE_CHANGES_INTERNALLY = false;
    private final NamespacedTimelockRpcClient delegate;
    private final NamespacedConjureTimelockService conjureDelegate;
    private final UUID clientId;
    private final CoalescingSupplier<LeaderTime> time;

    @VisibleForTesting
    LockLeaseService(
            NamespacedTimelockRpcClient timelockRpcClient,
            NamespacedConjureTimelockService conjureDelegate,
            UUID clientId) {
        this.delegate = timelockRpcClient;
        this.conjureDelegate = conjureDelegate;
        this.clientId = clientId;
        if (HAVE_ROLLED_OUT_CONJURE_CHANGES_INTERNALLY) {
            this.time = new CoalescingSupplier<>(conjureDelegate::leaderTime);
        } else {
            this.time = new CoalescingSupplier<>(delegate::getLeaderTime);
        }
    }

    static LockLeaseService create(
            NamespacedTimelockRpcClient timelockRpcClient,
            NamespacedConjureTimelockService conjureTimelock) {
        return new LockLeaseService(timelockRpcClient, conjureTimelock, UUID.randomUUID());
    }

    LockImmutableTimestampResponse lockImmutableTimestamp() {
        return startTransactions(1).immutableTimestamp();
    }

    StartTransactionResponseV4 startTransactions(int batchSize) {
        final StartTransactionResponseV4 response;
        if (HAVE_ROLLED_OUT_CONJURE_CHANGES_INTERNALLY) {
            ConjureStartTransactionsRequest request = ConjureStartTransactionsRequest.builder()
                    .requestorId(clientId)
                    .requestId(UUID.randomUUID())
                    .numTransactions(batchSize)
                    .build();
            ConjureStartTransactionsResponse conjureResponse = conjureDelegate.startTransactions(request);
            response = StartTransactionResponseV4.of(
                    conjureResponse.getImmutableTimestamp(),
                    conjureResponse.getTimestamps(),
                    conjureResponse.getLease());
        } else {
            StartTransactionRequestV4 request = StartTransactionRequestV4.createForRequestor(clientId, batchSize);
            response = delegate.startTransactions(request);
        }

        Lease lease = response.lease();
        LeasedLockToken leasedLockToken = LeasedLockToken.of(response.immutableTimestamp().getLock(), lease);
        long immutableTs = response.immutableTimestamp().getImmutableTimestamp();

        return StartTransactionResponseV4.of(
                LockImmutableTimestampResponse.of(immutableTs, leasedLockToken),
                response.timestamps(),
                lease);
    }

    //todo (gmaretic): clean this up once we have conjure undertow LW
    StartTransactionResponseV5 startTransactionsWithWatches(OptionalLong lastKnownVersion, int batchSize) {
        StartTransactionRequestV5 request = StartTransactionRequestV5
                .createForRequestor(clientId, lastKnownVersion, batchSize);
        StartTransactionResponseV5 response = delegate.startTransactionsWithWatches(request);

        Lease lease = response.lease();
        LeasedLockToken leasedLockToken = LeasedLockToken.of(response.immutableTimestamp().getLock(), lease);
        long immutableTs = response.immutableTimestamp().getImmutableTimestamp();

        return StartTransactionResponseV5.of(
                LockImmutableTimestampResponse.of(immutableTs, leasedLockToken),
                response.timestamps(),
                lease,
                response.lockWatchUpdate());
    }

    LockResponse lock(LockRequest request) {
        LockResponseV2 leasableResponse = delegate.lock(IdentifiedLockRequest.from(request));

        return leasableResponse.accept(LockResponseV2.Visitor.of(
                successful -> LockResponse.successful(
                        LeasedLockToken.of(successful.getToken(), successful.getLease())),
                unsuccessful -> LockResponse.timedOut()));
    }

    Set<LockToken> refreshLockLeases(Set<LockToken> uncastedTokens) {
        if (uncastedTokens.isEmpty()) {
            return uncastedTokens;
        }

        LeaderTime leaderTime = time.get();
        Set<LeasedLockToken> allTokens = leasedTokens(uncastedTokens);

        Set<LeasedLockToken> validByLease = allTokens.stream()
                .filter(token -> token.isValid(leaderTime))
                .collect(Collectors.toSet());

        Set<LeasedLockToken> toRefresh = Sets.difference(allTokens, validByLease);
        Set<LeasedLockToken> refreshedTokens = refreshTokens(toRefresh);

        return Sets.union(refreshedTokens, validByLease);
    }

    Set<LockToken> unlock(Set<LockToken> tokens) {
        Set<LeasedLockToken> leasedLockTokens = leasedTokens(tokens);
        leasedLockTokens.forEach(LeasedLockToken::invalidate);

        Set<LockToken> unlocked = delegate.unlock(serverTokens(leasedLockTokens));
        return leasedLockTokens.stream()
                .filter(leasedLockToken -> unlocked.contains(leasedLockToken.serverToken()))
                .collect(Collectors.toSet());
    }

    private Set<LeasedLockToken> refreshTokens(Set<LeasedLockToken> leasedTokens) {
        if (leasedTokens.isEmpty()) {
            return leasedTokens;
        }

        RefreshLockResponseV2 refreshLockResponse = delegate.refreshLockLeases(
                serverTokens(leasedTokens));
        Lease lease = refreshLockResponse.getLease();

        Set<LeasedLockToken> refreshedTokens = leasedTokens.stream()
                .filter(t -> refreshLockResponse.refreshedTokens().contains(t.serverToken()))
                .collect(Collectors.toSet());

        refreshedTokens.forEach(t -> t.updateLease(lease));
        return refreshedTokens;
    }

    @SuppressWarnings("unchecked")
    private static Set<LeasedLockToken> leasedTokens(Set<LockToken> tokens) {
        for (LockToken token : tokens) {
            Preconditions.checkArgument(
                    token instanceof LeasedLockToken,
                    "All lock tokens should be an instance of LeasedLockToken");
        }
        return (Set<LeasedLockToken>) (Set<?>) tokens;
    }

    private static Set<LockToken> serverTokens(Set<LeasedLockToken> leasedTokens) {
        return leasedTokens.stream()
                .map(LeasedLockToken::serverToken)
                .collect(Collectors.toSet());
    }
}
