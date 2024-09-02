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

import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampsRequestV2;
import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampsResponseV2;
import com.palantir.atlasdb.timelock.api.ConjureTimestampRange;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.lock.v2.ClientLockingOptions;
import com.palantir.lock.v2.GetCommitTimestampResponse;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.NamespacedTimelockRpcClient;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionResponse;
import com.palantir.lock.v2.TimelockService;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.lock.v2.WaitForLocksResponse;
import com.palantir.lock.watch.LockWatchCache;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.timestamp.TimestampRange;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public final class RemoteTimelockServiceAdapter implements TimelockService, AutoCloseable {
    private static final SafeLogger log = SafeLoggerFactory.get(RemoteTimelockServiceAdapter.class);

    private final NamespacedTimelockRpcClient rpcClient;
    private final NamespacedConjureTimelockService conjureTimelockService;
    private final LockLeaseService lockLeaseService;
    private final TransactionStarter transactionStarter;
    private final CommitTimestampGetter commitTimestampGetter;

    public RemoteTimelockServiceAdapter(
            NamespacedTimelockRpcClient rpcClient,
            NamespacedConjureTimelockService conjureTimelockService,
            LockLeaseService lockLeaseService,
            TransactionStarter transactionStarter,
            CommitTimestampGetter commitTimestampGetter) {
        this.rpcClient = rpcClient;
        this.conjureTimelockService = conjureTimelockService;
        this.lockLeaseService = lockLeaseService;
        this.transactionStarter = transactionStarter;
        this.commitTimestampGetter = commitTimestampGetter;
    }

    private RemoteTimelockServiceAdapter(
            NamespacedTimelockRpcClient rpcClient,
            NamespacedConjureTimelockService conjureTimelockService,
            LeaderTimeGetter leaderTimeGetter,
            RequestBatchersFactory batcherFactory,
            LockTokenUnlocker unlocker) {
        this.rpcClient = rpcClient;
        this.lockLeaseService = LockLeaseService.create(conjureTimelockService, leaderTimeGetter, unlocker);
        this.transactionStarter = TransactionStarter.create(lockLeaseService, batcherFactory);
        this.commitTimestampGetter = batcherFactory.createBatchingCommitTimestampGetter(lockLeaseService);
        this.conjureTimelockService = conjureTimelockService;
    }

    public static RemoteTimelockServiceAdapter create(
            Namespace namespace,
            NamespacedTimelockRpcClient rpcClient,
            NamespacedConjureTimelockService conjureClient,
            LockWatchCache lockWatchCache) {
        return create(
                rpcClient,
                conjureClient,
                new LegacyLeaderTimeGetter(conjureClient),
                RequestBatchersFactory.create(lockWatchCache, namespace, Optional.empty()),
                new LegacyLockTokenUnlocker(conjureClient));
    }

    public static RemoteTimelockServiceAdapter create(
            NamespacedTimelockRpcClient rpcClient,
            NamespacedConjureTimelockService conjureClient,
            LeaderTimeGetter leaderTimeGetter,
            RequestBatchersFactory batcherFactory,
            LockTokenUnlocker unlocker) {
        return new RemoteTimelockServiceAdapter(rpcClient, conjureClient, leaderTimeGetter, batcherFactory, unlocker);
    }

    @Override
    public long getFreshTimestamp() {
        return conjureTimelockService.getFreshTimestamp().get();
    }

    @Override
    public GetCommitTimestampResponse getCommitTimestamp(long startTs, LockToken commitLocksToken) {
        return commitTimestampGetter.getCommitTimestamp(startTs, commitLocksToken);
    }

    @Override
    public TimestampRange getFreshTimestamps(int numTimestampsRequested) {
        ConjureGetFreshTimestampsResponseV2 response = conjureTimelockService.getFreshTimestampsV2(
                ConjureGetFreshTimestampsRequestV2.of(numTimestampsRequested));
        ConjureTimestampRange timestampRange = response.get();
        return TimestampRange.createRangeFromDeltaEncoding(timestampRange.getStart(), timestampRange.getCount());
    }

    @Override
    public long getImmutableTimestamp() {
        return rpcClient.getImmutableTimestamp();
    }

    @Override
    public long getCommitImmutableTimestamp() {
        return rpcClient.getCommitImmutableTimestamp();
    }

    @Override
    public WaitForLocksResponse waitForLocks(WaitForLocksRequest request) {
        return lockLeaseService.waitForLocks(request);
    }

    @Override
    public LockImmutableTimestampResponse lockImmutableTimestamp() {
        return lockLeaseService.lockImmutableTimestamp();
    }

    @Override
    public LockResponse lock(LockRequest request) {
        return lockLeaseService.lock(request);
    }

    @Override
    public LockResponse lock(LockRequest lockRequest, ClientLockingOptions options) {
        log.warn(
                "Locking with client options should not happen at the level of the remote adapter. We will perform a"
                        + " normal lock, disregarding these options here.",
                new SafeRuntimeException("I exist to show you the stack trace"));
        return lockLeaseService.lock(lockRequest);
    }

    @Override
    public List<StartIdentifiedAtlasDbTransactionResponse> startIdentifiedAtlasDbTransactionBatch(int count) {
        return transactionStarter.startIdentifiedAtlasDbTransactionBatch(count);
    }

    @Override
    public Set<LockToken> refreshLockLeases(Set<LockToken> tokens) {
        return transactionStarter.refreshLockLeases(tokens);
    }

    @Override
    public Set<LockToken> unlock(Set<LockToken> tokens) {
        return transactionStarter.unlock(tokens);
    }

    @Override
    public void tryUnlock(Set<LockToken> tokens) {
        unlock(tokens);
    }

    @Override
    public long currentTimeMillis() {
        return rpcClient.currentTimeMillis();
    }

    @Override
    public void close() {
        transactionStarter.close();
        commitTimestampGetter.close();
        lockLeaseService.close();
    }
}
