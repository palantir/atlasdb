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
package com.palantir.atlasdb.timelock;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsRequest;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsResponse;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsResponse;
import com.palantir.atlasdb.timelock.lock.watch.LockWatchingService;
import com.palantir.lock.client.IdentifiedLockRequest;
import com.palantir.lock.v2.IdentifiedTimeLockRequest;
import com.palantir.lock.v2.ImmutableStartTransactionResponseV4;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockResponseV2;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.RefreshLockResponseV2;
import com.palantir.lock.v2.StartAtlasDbTransactionResponse;
import com.palantir.lock.v2.StartAtlasDbTransactionResponseV3;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionRequest;
import com.palantir.lock.v2.StartTransactionRequestV4;
import com.palantir.lock.v2.StartTransactionResponseV4;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.lock.v2.WaitForLocksResponse;
import com.palantir.lock.watch.LockWatchVersion;
import com.palantir.timestamp.ManagedTimestampService;
import com.palantir.timestamp.TimestampRange;
import java.io.Closeable;
import java.util.Optional;
import java.util.Set;

public interface AsyncTimelockService
        extends BackupTimeLockServiceView, ManagedTimestampService, LockWatchingService, Closeable {

    long currentTimeMillis();

    ListenableFuture<WaitForLocksResponse> waitForLocks(WaitForLocksRequest request);

    ListenableFuture<LockResponseV2> lock(IdentifiedLockRequest request);

    long getImmutableTimestamp();

    StartAtlasDbTransactionResponse deprecatedStartTransaction(IdentifiedTimeLockRequest request);

    StartAtlasDbTransactionResponseV3 startTransaction(StartIdentifiedAtlasDbTransactionRequest request);

    StartTransactionResponseV4 startTransactions(StartTransactionRequestV4 request);

    ListenableFuture<ConjureStartTransactionsResponse> startTransactionsWithWatches(
            ConjureStartTransactionsRequest request);

    ListenableFuture<GetCommitTimestampsResponse> getCommitTimestamps(
            int numTimestamps, Optional<LockWatchVersion> lastKnownVersion);

    ListenableFuture<LeaderTime> leaderTime();

    ListenableFuture<TimestampRange> getFreshTimestampsAsync(int timestampsToRequest);

    default ListenableFuture<Long> getFreshTimestampAsync() {
        return Futures.transform(
                getFreshTimestampsAsync(1), TimestampRange::getLowerBound, MoreExecutors.directExecutor());
    }

    default ListenableFuture<StartTransactionResponseV4> startTransactionsAsync(StartTransactionRequestV4 request) {
        ConjureStartTransactionsRequest conjureRequest = ConjureStartTransactionsRequest.builder()
                .requestId(request.requestId())
                .requestorId(request.requestorId())
                .numTransactions(request.numTransactions())
                .build();
        return Futures.transform(
                startTransactionsWithWatches(conjureRequest),
                newResponse -> ImmutableStartTransactionResponseV4.builder()
                        .timestamps(newResponse.getTimestamps())
                        .immutableTimestamp(newResponse.getImmutableTimestamp())
                        .lease(newResponse.getLease())
                        .build(),
                MoreExecutors.directExecutor());
    }

    default ListenableFuture<LockResponse> deprecatedLock(IdentifiedLockRequest request) {
        return Futures.transform(
                lock(request),
                result -> result.accept(LockResponseV2.Visitor.of(
                        success -> LockResponse.successful(success.getToken()),
                        unsuccessful -> LockResponse.timedOut())),
                MoreExecutors.directExecutor());
    }

    default ListenableFuture<Set<LockToken>> deprecatedRefreshLockLeases(Set<LockToken> tokens) {
        return Futures.transform(
                refreshLockLeases(tokens), RefreshLockResponseV2::refreshedTokens, MoreExecutors.directExecutor());
    }
}
