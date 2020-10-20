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

import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsRequest;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsResponse;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsResponse;
import com.palantir.atlasdb.timelock.lock.watch.LockWatchingService;
import com.palantir.lock.client.IdentifiedLockRequest;
import com.palantir.lock.v2.IdentifiedTimeLockRequest;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
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

public interface AsyncTimelockService extends ManagedTimestampService, LockWatchingService, Closeable {

    long currentTimeMillis();

    ListenableFuture<Set<LockToken>> unlock(Set<LockToken> tokens);

    ListenableFuture<RefreshLockResponseV2> refreshLockLeases(Set<LockToken> tokens);

    ListenableFuture<WaitForLocksResponse> waitForLocks(WaitForLocksRequest request);

    ListenableFuture<LockResponseV2> lock(IdentifiedLockRequest request);

    long getImmutableTimestamp();

    LockImmutableTimestampResponse lockImmutableTimestamp(IdentifiedTimeLockRequest request);

    StartAtlasDbTransactionResponse deprecatedStartTransaction(IdentifiedTimeLockRequest request);

    StartAtlasDbTransactionResponseV3 startTransaction(StartIdentifiedAtlasDbTransactionRequest request);

    StartTransactionResponseV4 startTransactions(StartTransactionRequestV4 request);

    ListenableFuture<ConjureStartTransactionsResponse> startTransactionsWithWatches(
            ConjureStartTransactionsRequest request);

    ListenableFuture<GetCommitTimestampsResponse> getCommitTimestamps(
            int numTimestamps, Optional<LockWatchVersion> lastKnownVersion);

    ListenableFuture<LeaderTime> leaderTime();

    ListenableFuture<TimestampRange> getFreshTimestampsAsync(int timestampsToRequest);
}
