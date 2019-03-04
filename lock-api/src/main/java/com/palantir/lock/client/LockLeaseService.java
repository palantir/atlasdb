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

import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.palantir.common.concurrent.CoalescingSupplier;
import com.palantir.lock.v2.BatchedStartTransactionRequest;
import com.palantir.lock.v2.BatchedStartTransactionResponse;
import com.palantir.lock.v2.ImmutableLockImmutableTimestampResponse;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.lock.v2.Lease;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockResponseV2;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.RefreshLockResponseV2;
import com.palantir.lock.v2.StartAtlasDbTransactionResponseV3;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionRequest;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionResponse;
import com.palantir.lock.v2.TimelockRpcClient;
import com.palantir.logsafe.Preconditions;

final class LockLeaseService {
    private final TimelockRpcClient delegate;
    private final UUID clientId;
    private final CoalescingSupplier<LeaderTime> time;

    @VisibleForTesting
    LockLeaseService(TimelockRpcClient timelockRpcClient, UUID clientId) {
        this.delegate = timelockRpcClient;
        this.clientId = clientId;
        this.time = new CoalescingSupplier<>(timelockRpcClient::getLeaderTime);
    }

    public static LockLeaseService create(TimelockRpcClient timelockRpcClient) {
        return new LockLeaseService(timelockRpcClient, UUID.randomUUID());
    }

    public LockImmutableTimestampResponse lockImmutableTimestamp() {
        StartAtlasDbTransactionResponseV3 response = delegate.deprecatedStartTransaction(
                StartIdentifiedAtlasDbTransactionRequest.createForRequestor(clientId));

        return ImmutableLockImmutableTimestampResponse.of(
                response.immutableTimestamp().getImmutableTimestamp(),
                LeasedLockToken.of(response.immutableTimestamp().getLock(), response.getLease()));
    }

    public StartIdentifiedAtlasDbTransactionResponse startIdentifiedAtlasDbTransaction() {
        StartAtlasDbTransactionResponseV3 response = delegate.deprecatedStartTransaction(
                StartIdentifiedAtlasDbTransactionRequest.createForRequestor(clientId));

        Lease lease = response.getLease();
        LeasedLockToken leasedLockToken = LeasedLockToken.of(response.immutableTimestamp().getLock(), lease);
        long immutableTs = response.immutableTimestamp().getImmutableTimestamp();

        return StartIdentifiedAtlasDbTransactionResponse.of(
                LockImmutableTimestampResponse.of(immutableTs, leasedLockToken),
                response.startTimestampAndPartition());
    }

    public BatchedStartTransactionResponse batchedStartTransaction(int batchSize) {
        BatchedStartTransactionRequest request = BatchedStartTransactionRequest.createForRequestor(clientId, batchSize);
        BatchedStartTransactionResponse response = delegate.batchedStartTransaction(request);

        Lease lease = response.lease();
        LeasedLockToken leasedLockToken = LeasedLockToken.of(response.immutableTimestamp().getLock(), lease);
        long immutableTs = response.immutableTimestamp().getImmutableTimestamp();

        return BatchedStartTransactionResponse.of(
                LockImmutableTimestampResponse.of(immutableTs, leasedLockToken),
                response.startTimestamps(),
                lease);
    }

    public LockResponse lock(LockRequest request) {
        LockResponseV2 leasableResponse = delegate.lock(IdentifiedLockRequest.from(request));

        return leasableResponse.accept(LockResponseV2.Visitor.of(
                successful -> LockResponse.successful(
                        LeasedLockToken.of(successful.getToken(), successful.getLease())),
                unsuccessful -> LockResponse.timedOut()));
    }

    public Set<LockToken> refreshLockLeases(Set<LockToken> uncastedTokens) {
        LeaderTime leaderTime = time.get();
        Set<LeasedLockToken> allTokens = leasedTokens(uncastedTokens);

        Set<LeasedLockToken> validByLease = allTokens.stream()
                .filter(token -> token.isValid(leaderTime))
                .collect(Collectors.toSet());

        Set<LeasedLockToken> toRefresh = Sets.difference(allTokens, validByLease);
        Set<LeasedLockToken> refreshedTokens = refreshTokens(toRefresh);

        return Sets.union(refreshedTokens, validByLease);
    }

    public Set<LockToken> unlock(Set<LockToken> tokens) {
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
        Preconditions.checkArgument(tokens.stream()
                        .allMatch(token -> token instanceof LeasedLockToken),
                "All lock tokens should be an instance of LeasedLockToken");
        return (Set<LeasedLockToken>) (Set<?>) tokens;
    }

    private static Set<LockToken> serverTokens(Set<LeasedLockToken> leasedTokens) {
        return leasedTokens.stream()
                .map(LeasedLockToken::serverToken)
                .collect(Collectors.toSet());
    }
}
