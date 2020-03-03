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

import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.timelock.api.ConjureLockResponse;
import com.palantir.atlasdb.timelock.api.ConjureLockToken;
import com.palantir.atlasdb.timelock.api.ConjureRefreshLocksRequest;
import com.palantir.atlasdb.timelock.api.ConjureRefreshLocksResponse;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsRequest;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsResponse;
import com.palantir.atlasdb.timelock.api.ConjureUnlockRequest;
import com.palantir.atlasdb.timelock.api.SuccessfulLockResponse;
import com.palantir.atlasdb.timelock.api.UnsuccessfulLockResponse;
import com.palantir.common.concurrent.CoalescingSupplier;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.lock.v2.Lease;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.StartTransactionResponseV4;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;

class LockLeaseService {
    private final NamespacedConjureTimelockService delegate;
    private final UUID clientId;
    private final CoalescingSupplier<LeaderTime> time;

    @VisibleForTesting
    LockLeaseService(
            NamespacedConjureTimelockService delegate,
            UUID clientId) {
        this.delegate = delegate;
        this.clientId = clientId;
        this.time = new CoalescingSupplier<>(delegate::leaderTime);
    }

    static LockLeaseService create(NamespacedConjureTimelockService conjureTimelock) {
        return new LockLeaseService(conjureTimelock, UUID.randomUUID());
    }

    LockImmutableTimestampResponse lockImmutableTimestamp() {
        return startTransactions(1).immutableTimestamp();
    }

    StartTransactionResponseV4 startTransactions(int batchSize) {
        ConjureStartTransactionsRequest request = ConjureStartTransactionsRequest.builder()
                .requestorId(clientId)
                .requestId(UUID.randomUUID())
                .numTransactions(batchSize)
                .lastKnownVersion(Optional.empty())
                .build();
        ConjureStartTransactionsResponse conjureResponse = delegate.startTransactions(request);
        StartTransactionResponseV4 response = StartTransactionResponseV4.of(
                conjureResponse.getImmutableTimestamp(),
                conjureResponse.getTimestamps(),
                conjureResponse.getLease());

        Lease lease = response.lease();
        LeasedLockToken leasedLockToken =
                LeasedLockToken.of(ConjureLockToken.of(response.immutableTimestamp().getLock().getRequestId()), lease);
        long immutableTs = response.immutableTimestamp().getImmutableTimestamp();

        return StartTransactionResponseV4.of(
                LockImmutableTimestampResponse.of(immutableTs, leasedLockToken),
                response.timestamps(),
                lease);
    }

    LockResponse lock(LockRequest request) {
        return delegate.lock(ConjureLockRequests.toConjure(request)).accept(ToLeasedLockResponse.INSTANCE);
    }

    private enum ToLeasedLockResponse implements ConjureLockResponse.Visitor<LockResponse> {
        INSTANCE;

        @Override
        public LockResponse visitSuccessful(SuccessfulLockResponse value) {
            return LockResponse.successful(LeasedLockToken.of(value.getLockToken(), value.getLease()));
        }

        @Override
        public LockResponse visitUnsuccessful(UnsuccessfulLockResponse value) {
            return LockResponse.timedOut();
        }

        @Override
        public LockResponse visitUnknown(String unknownType) {
            throw new SafeIllegalStateException("Unknown response type", SafeArg.of("type", unknownType));
        }
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

        Set<LockToken> unlocked = delegate.unlock(ConjureUnlockRequest.of(serverTokens(leasedLockTokens))).getTokens()
                .stream()
                .map(ConjureLockToken::getRequestId)
                .map(LockToken::of)
                .collect(Collectors.toSet());
        return leasedLockTokens.stream()
                .filter(leasedLockToken -> unlocked.contains(leasedLockToken.serverToken()))
                .collect(Collectors.toSet());
    }

    private Set<LeasedLockToken> refreshTokens(Set<LeasedLockToken> leasedTokens) {
        if (leasedTokens.isEmpty()) {
            return leasedTokens;
        }

        ConjureRefreshLocksResponse refreshLockResponse = delegate.refreshLocks(
                ConjureRefreshLocksRequest.of(serverTokens(leasedTokens)));
        Lease lease = refreshLockResponse.getLease();

        Set<LeasedLockToken> refreshedTokens = leasedTokens.stream()
                .filter(t -> refreshLockResponse.getRefreshedTokens().contains(t.serverToken()))
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

    private static Set<ConjureLockToken> serverTokens(Set<LeasedLockToken> leasedTokens) {
        return leasedTokens.stream()
                .map(LeasedLockToken::serverToken)
                .collect(Collectors.toSet());
    }

    private static ConjureLockToken toConjure(LockToken lockToken) {
        return ConjureLockToken.of(lockToken.getRequestId());
    }
}
