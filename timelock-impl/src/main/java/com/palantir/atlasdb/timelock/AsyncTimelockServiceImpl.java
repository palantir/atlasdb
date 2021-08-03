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
import com.google.common.util.concurrent.SettableFuture;
import com.palantir.atlasdb.timelock.api.ConjureIdentifiedVersion;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsRequest;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsResponse;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsResponse;
import com.palantir.atlasdb.timelock.api.LockWatchRequest;
import com.palantir.atlasdb.timelock.lock.AsyncLockService;
import com.palantir.atlasdb.timelock.lock.AsyncResult;
import com.palantir.atlasdb.timelock.lock.Leased;
import com.palantir.atlasdb.timelock.lock.LockLog;
import com.palantir.atlasdb.timelock.lock.TimeLimit;
import com.palantir.atlasdb.timelock.lock.watch.ValueAndLockWatchStateUpdate;
import com.palantir.atlasdb.timelock.transaction.timestamp.DelegatingClientAwareManagedTimestampService;
import com.palantir.atlasdb.timelock.transaction.timestamp.LeadershipGuardedClientAwareManagedTimestampService;
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
import com.palantir.lock.v2.StartTransactionResponseV4;
import com.palantir.lock.v2.TimestampAndPartition;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.lock.v2.WaitForLocksResponse;
import com.palantir.lock.watch.LockWatchStateUpdate;
import com.palantir.lock.watch.LockWatchVersion;
import com.palantir.timestamp.ManagedTimestampService;
import com.palantir.timestamp.TimestampRange;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

public class AsyncTimelockServiceImpl implements AsyncTimelockService {
    private final AsyncLockService lockService;
    private final LeadershipGuardedClientAwareManagedTimestampService timestampService;
    private final LockLog lockLog;

    public AsyncTimelockServiceImpl(
            AsyncLockService lockService, ManagedTimestampService timestampService, LockLog lockLog) {
        this.lockService = lockService;
        this.timestampService = new LeadershipGuardedClientAwareManagedTimestampService(
                DelegatingClientAwareManagedTimestampService.createDefault(timestampService));
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
    public LockImmutableTimestampResponse lockSpecificImmutableTimestamp(
            IdentifiedTimeLockRequest request, long timestamp) {
        Leased<LockImmutableTimestampResponse> leasedLockImmutableTimestampResponse =
                lockImmutableTimestampWithLease(request.getRequestId(), timestamp);

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
                request.getRequestId(), request.getLockDescriptors(), TimeLimit.of(request.getAcquireTimeoutMs()));
        lockLog.registerRequest(request, result);
        SettableFuture<LockResponseV2> response = SettableFuture.create();
        result.onComplete(() -> {
            if (result.isFailed()) {
                response.setException(result.getError());
            } else if (result.isTimedOut()) {
                response.set(LockResponseV2.timedOut());
            } else {
                response.set(LockResponseV2.successful(
                        result.get().value(), result.get().lease()));
            }
        });
        return response;
    }

    @Override
    public ListenableFuture<WaitForLocksResponse> waitForLocks(WaitForLocksRequest request) {
        AsyncResult<Void> result = lockService.waitForLocks(
                request.getRequestId(), request.getLockDescriptors(), TimeLimit.of(request.getAcquireTimeoutMs()));
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
        return StartAtlasDbTransactionResponse.of(lockImmutableTimestamp(request), getFreshTimestamp());
    }

    @Override
    public StartAtlasDbTransactionResponseV3 startTransaction(StartIdentifiedAtlasDbTransactionRequest request) {
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

        Leased<LockToken> leasedLock =
                lockService.lockImmutableTimestamp(requestId, timestamp).get();
        long immutableTs = lockService.getImmutableTimestamp().orElse(timestamp);

        LockImmutableTimestampResponse lockImmutableTimestampResponse =
                LockImmutableTimestampResponse.of(immutableTs, leasedLock.value());

        return Leased.of(lockImmutableTimestampResponse, leasedLock.lease());
    }

    // TODO (jkong): Horrible
    private Leased<LockImmutableTimestampResponse> lockImmutableTimestampWithLease(UUID requestId, long lowerBound) {
        Leased<LockToken> leasedLock =
                lockService.lockImmutableTimestamp(requestId, lowerBound).get();
        long immutableTs = lockService.getImmutableTimestamp().orElse(lowerBound);

        LockImmutableTimestampResponse lockImmutableTimestampResponse =
                LockImmutableTimestampResponse.of(immutableTs, leasedLock.value());

        return Leased.of(lockImmutableTimestampResponse, leasedLock.lease());
    }

    @Override
    public ListenableFuture<ConjureStartTransactionsResponse> startTransactionsWithWatches(
            ConjureStartTransactionsRequest request) {
        return Futures.immediateFuture(startTransactionsWithWatchesSync(request));
    }

    private ConjureStartTransactionsResponse startTransactionsWithWatchesSync(ConjureStartTransactionsRequest request) {
        Leased<LockImmutableTimestampResponse> leasedLockImmutableTimestampResponse =
                lockImmutableTimestampWithLease(request.getRequestId());

        ValueAndLockWatchStateUpdate<PartitionedTimestamps> timestampsAndUpdate = lockService
                .getLockWatchingService()
                .runTask(
                        request.getLastKnownVersion().map(AsyncTimelockServiceImpl::fromConjure),
                        () -> timestampService.getFreshTimestampsForClient(
                                request.getRequestorId(), request.getNumTransactions()));

        return ConjureStartTransactionsResponse.builder()
                .immutableTimestamp(leasedLockImmutableTimestampResponse.value())
                .timestamps(timestampsAndUpdate.value())
                .lease(leasedLockImmutableTimestampResponse.lease())
                .lockWatchUpdate(timestampsAndUpdate.lockWatchStateUpdate())
                .build();
    }

    @Override
    public ListenableFuture<GetCommitTimestampsResponse> getCommitTimestamps(
            int numTimestamps, Optional<LockWatchVersion> lastKnownVersion) {
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
        timestampService.close();
        lockService.close();
    }

    @Override
    public void startWatching(LockWatchRequest locksToWatch) {
        lockService.getLockWatchingService().startWatching(locksToWatch);
    }

    @Override
    public LockWatchStateUpdate getWatchStateUpdate(Optional<LockWatchVersion> lastKnownVersion) {
        return lockService.getLockWatchingService().getWatchStateUpdate(lastKnownVersion);
    }

    @Override
    public <T> ValueAndLockWatchStateUpdate<T> runTask(Optional<LockWatchVersion> lastKnownVersion, Supplier<T> task) {
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

    private static LockWatchVersion fromConjure(ConjureIdentifiedVersion conjure) {
        return LockWatchVersion.of(conjure.getId(), conjure.getVersion());
    }
}
