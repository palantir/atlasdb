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

import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsResponse;
import com.palantir.atlasdb.timelock.api.LockWatchRequest;
import com.palantir.atlasdb.timelock.lock.AsyncLockService;
import com.palantir.atlasdb.timelock.lock.AsyncResult;
import com.palantir.atlasdb.timelock.lock.Leased;
import com.palantir.atlasdb.timelock.lock.LockLog;
import com.palantir.atlasdb.timelock.lock.TimeLimit;
import com.palantir.atlasdb.timelock.lock.watch.ValueAndVersion;
import com.palantir.atlasdb.timelock.transaction.timestamp.ClientAwareManagedTimestampService;
import com.palantir.atlasdb.timelock.transaction.timestamp.DelegatingClientAwareManagedTimestampService;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.client.IdentifiedLockRequest;
import com.palantir.lock.v2.IdentifiedTimeLockRequest;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockResponseV2;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.PartitionedTimestamps;
import com.palantir.lock.v2.RefreshLockResponseV2;
import com.palantir.lock.v2.StartAtlasDbTransactionResponse;
import com.palantir.lock.v2.StartAtlasDbTransactionResponseV3;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionRequest;
import com.palantir.lock.v2.StartTransactionRequestV4;
import com.palantir.lock.v2.StartTransactionRequestV5;
import com.palantir.lock.v2.StartTransactionResponseV4;
import com.palantir.lock.v2.StartTransactionResponseV5;
import com.palantir.lock.v2.TimestampAndPartition;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.lock.v2.WaitForLocksResponse;
import com.palantir.lock.watch.IdentifiedVersion;
import com.palantir.lock.watch.LockWatchStateUpdate;
import com.palantir.timestamp.ManagedTimestampService;
import com.palantir.timestamp.TimestampRange;

public class AsyncTimelockServiceImpl implements AsyncTimelockService {
    private final AsyncLockService lockService;
    private final ClientAwareManagedTimestampService timestampService;
    private final LockLog lockLog;

    public AsyncTimelockServiceImpl(
            AsyncLockService lockService,
            ManagedTimestampService timestampService,
            LockLog lockLog) {
        this.lockService = lockService;
        this.timestampService = DelegatingClientAwareManagedTimestampService.createDefault(timestampService);
        this.lockLog = lockLog;
    }

    @Override
    public boolean isInitialized() {
        return timestampService.isInitialized();
    }

    @Override
    public long getFreshTimestamp() {
        return timestampService.getFreshTimestamp();
    }

    @Override
    public TimestampRange getFreshTimestamps(int numTimestampsRequested) {
        return timestampService.getFreshTimestamps(numTimestampsRequested);
    }

    @Override
    public LockImmutableTimestampResponse lockImmutableTimestamp(IdentifiedTimeLockRequest request) {
        Leased<LockImmutableTimestampResponse> leasedLockImmutableTimestampResponse =
                lockImmutableTimestampWithLease(request.getRequestId());

        return leasedLockImmutableTimestampResponse.value();
    }

    @Override
    public long getImmutableTimestamp() {
        long timestamp = timestampService.getFreshTimestamp();
        return lockService.getImmutableTimestamp().orElse(timestamp);
    }

    @Override
    public ListenableFuture<LockResponseV2> lock(IdentifiedLockRequest request) {
        AsyncResult<Leased<LockToken>> result = lockService.lock(
                request.getRequestId(),
                request.getLockDescriptors(),
                TimeLimit.of(request.getAcquireTimeoutMs()));
        lockLog.registerRequest(request, result);
        SettableFuture<LockResponseV2> response = SettableFuture.create();
        result.onComplete(() -> {
            if (result.isFailed()) {
                response.setException(result.getError());
            } else if (result.isTimedOut()) {
                response.set(LockResponseV2.timedOut());
            } else {
                response.set(LockResponseV2.successful(result.get().value(), result.get().lease()));
            }
        });
        return response;
    }

    @Override
    public ListenableFuture<WaitForLocksResponse> waitForLocks(WaitForLocksRequest request) {
        AsyncResult<Void> result = lockService.waitForLocks(
                request.getRequestId(),
                request.getLockDescriptors(),
                TimeLimit.of(request.getAcquireTimeoutMs()));
        lockLog.registerRequest(request, result);
        SettableFuture<WaitForLocksResponse> response = SettableFuture.create();
        result.onComplete(() -> {
            if (result.isFailed()) {
                response.setException(result.getError());
            } else if (result.isTimedOut()) {
                response.set(WaitForLocksResponse.timedOut());
            } else {
                response.set(WaitForLocksResponse.successful());
            }
        });
        return response;
    }

    @Override
    public ListenableFuture<RefreshLockResponseV2> refreshLockLeases(Set<LockToken> tokens) {
        return Futures.immediateFuture(lockService.refresh(tokens));
    }

    @Override
    public ListenableFuture<Set<LockToken>> unlock(Set<LockToken> tokens) {
        return Futures.immediateFuture(lockService.unlock(tokens));
    }

    @Override
    public StartAtlasDbTransactionResponse deprecatedStartTransaction(IdentifiedTimeLockRequest request) {
        return StartAtlasDbTransactionResponse.of(
                lockImmutableTimestamp(request),
                getFreshTimestamp());
    }

    @Override
    public StartAtlasDbTransactionResponseV3 startTransaction(
            StartIdentifiedAtlasDbTransactionRequest request) {
        StartTransactionResponseV4 startTransactionResponseV4 =
                startTransactions(StartTransactionRequestV4.createForRequestor(request.requestorId(), 1));

        return StartAtlasDbTransactionResponseV3.of(
                startTransactionResponseV4.immutableTimestamp(),
                getTimestampAndPartition(startTransactionResponseV4.timestamps()),
                startTransactionResponseV4.lease());
    }

    private static TimestampAndPartition getTimestampAndPartition(PartitionedTimestamps partitionedTimestamps) {
        return TimestampAndPartition.of(partitionedTimestamps.start(), partitionedTimestamps.partition());
    }

    @Override
    public StartTransactionResponseV4 startTransactions(StartTransactionRequestV4 request) {
        Leased<LockImmutableTimestampResponse> leasedLockImmutableTimestampResponse =
                lockImmutableTimestampWithLease(request.requestId());

        PartitionedTimestamps partitionedTimestamps =
                timestampService.getFreshTimestampsForClient(request.requestorId(), request.numTransactions());

        return StartTransactionResponseV4.of(
                leasedLockImmutableTimestampResponse.value(),
                partitionedTimestamps,
                leasedLockImmutableTimestampResponse.lease());
    }

    private Leased<LockImmutableTimestampResponse> lockImmutableTimestampWithLease(UUID requestId) {
        long timestamp = timestampService.getFreshTimestamp();

        Leased<LockToken> leasedLock = lockService.lockImmutableTimestamp(requestId, timestamp).get();
        long immutableTs = lockService.getImmutableTimestamp().orElse(timestamp);

        LockImmutableTimestampResponse lockImmutableTimestampResponse =
                LockImmutableTimestampResponse.of(immutableTs, leasedLock.value());

        return Leased.of(lockImmutableTimestampResponse, leasedLock.lease());
    }

    @Override
    public ListenableFuture<StartTransactionResponseV5> startTransactionsWithWatches(StartTransactionRequestV5 request) {
        return Futures.immediateFuture(startTransactionsWithWatchesSync(request));
    }

    private StartTransactionResponseV5 startTransactionsWithWatchesSync(StartTransactionRequestV5 request) {
        Leased<LockImmutableTimestampResponse> leasedLockImmutableTimestampResponse =
                lockImmutableTimestampWithLease(request.requestId());

        ValueAndVersion<PartitionedTimestamps> timestampsAndVersion = lockService.getLockWatchingService()
                .runTaskAndAtomicallyReturnLockWatchVersion(() ->
                        timestampService.getFreshTimestampsForClient(request.requestorId(), request.numTransactions()));

        return StartTransactionResponseV5.of(
                leasedLockImmutableTimestampResponse.value(),
                timestampsAndVersion.value(),
                leasedLockImmutableTimestampResponse.lease(),
                getWatchStateUpdate(request.lastKnownLockLogVersion(), timestampsAndVersion.version()));
    }

    @Override
    public ListenableFuture<GetCommitTimestampsResponse> getCommitTimestamps(
            int numTimestamps, Optional<IdentifiedVersion> lastKnownVersion) {
        TimestampRange freshTimestamps = getFreshTimestamps(numTimestamps);
        return Futures.immediateFuture(GetCommitTimestampsResponse.of(
                freshTimestamps.getLowerBound(),
                freshTimestamps.getUpperBound(),
                getWatchStateUpdate(lastKnownVersion)));
    }

    @Override
    public ListenableFuture<LeaderTime> leaderTime() {
        return Futures.immediateFuture(lockService.leaderTime());
    }

    @Override
    public ListenableFuture<TimestampRange> getFreshTimestampsAsync(int timestampsToRequest) {
        return Futures.immediateFuture(getFreshTimestamps(timestampsToRequest));
    }

    @Override
    public long currentTimeMillis() {
        return System.currentTimeMillis();
    }

    @Override
    public void fastForwardTimestamp(long currentTimestamp) {
        timestampService.fastForwardTimestamp(currentTimestamp);
    }

    @Override
    public String ping() {
        return timestampService.ping();
    }

    @Override
    public void close() {
        lockService.close();
    }

    @Override
    public void startWatching(LockWatchRequest locksToWatch) {
        lockService.getLockWatchingService().startWatching(locksToWatch);
    }

    @Override
    public LockWatchStateUpdate getWatchStateUpdate(Optional<IdentifiedVersion> lastKnownVersion) {
        return lockService.getLockWatchingService().getWatchStateUpdate(lastKnownVersion);
    }

    @Override
    public LockWatchStateUpdate getWatchStateUpdate(Optional<IdentifiedVersion> lastKnownVersion, long endVersion) {
        return lockService.getLockWatchingService().getWatchStateUpdate(lastKnownVersion, endVersion);
    }

    @Override
    public <T> ValueAndVersion<T> runTaskAndAtomicallyReturnLockWatchVersion(Supplier<T> task) {
        throw new UnsupportedOperationException("Exposing this method is too dangerous.");
    }

    @Override
    public void registerLock(Set<LockDescriptor> locksTakenOut, LockToken token) {
        lockService.getLockWatchingService().registerLock(locksTakenOut, token);
    }

    @Override
    public void registerUnlock(Set<LockDescriptor> locksUnlocked) {
        lockService.getLockWatchingService().registerUnlock(locksUnlocked);
    }
}
