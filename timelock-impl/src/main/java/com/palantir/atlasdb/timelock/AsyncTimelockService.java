/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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

import java.io.Closeable;
import java.util.OptionalLong;
import java.util.Set;

import com.palantir.atlasdb.timelock.lock.AsyncResult;
import com.palantir.atlasdb.timelock.lock.Leased;
import com.palantir.atlasdb.timelock.lock.watch.LockWatchingService;
import com.palantir.lock.client.IdentifiedLockRequest;
import com.palantir.lock.v2.IdentifiedTimeLockRequest;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.RefreshLockResponseV2;
import com.palantir.lock.v2.StartAtlasDbTransactionResponse;
import com.palantir.lock.v2.StartAtlasDbTransactionResponseV3;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionRequest;
import com.palantir.lock.v2.StartTransactionRequestV4;
import com.palantir.lock.v2.StartTransactionRequestV5;
import com.palantir.lock.v2.StartTransactionResponseV4;
import com.palantir.lock.v2.StartTransactionResponseV5;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.lock.watch.TimestampWithWatches;
import com.palantir.timestamp.ManagedTimestampService;

public interface AsyncTimelockService extends ManagedTimestampService, LockWatchingService, Closeable {

    long currentTimeMillis();

    Set<LockToken> unlock(Set<LockToken> tokens);

    RefreshLockResponseV2 refreshLockLeases(Set<LockToken> tokens);

    AsyncResult<Void> waitForLocks(WaitForLocksRequest request);

    AsyncResult<Leased<LockToken>> lock(IdentifiedLockRequest request);

    long getImmutableTimestamp();

    LockImmutableTimestampResponse lockImmutableTimestamp(IdentifiedTimeLockRequest request);

    StartAtlasDbTransactionResponse deprecatedStartTransaction(IdentifiedTimeLockRequest request);

    StartAtlasDbTransactionResponseV3 startTransaction(StartIdentifiedAtlasDbTransactionRequest request);

    StartTransactionResponseV4 startTransactions(StartTransactionRequestV4 request);

    StartTransactionResponseV5 startTransactionsWithWatches(StartTransactionRequestV5 request);

    TimestampWithWatches getCommitTimestampWithWatches(OptionalLong lastKnownVersion);

    LeaderTime leaderTime();
}
