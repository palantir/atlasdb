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

import com.palantir.atlasdb.common.api.timelock.TimestampLeaseName;
import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampsRequestV2;
import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampsResponseV2;
import com.palantir.atlasdb.timelock.api.ConjureTimelockService;
import com.palantir.atlasdb.timelock.api.ConjureTimestampRange;
import com.palantir.atlasdb.timelock.api.MultiClientConjureTimelockServiceBlocking;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.lock.client.timestampleases.MinLeasedTimestampGetter;
import com.palantir.lock.client.timestampleases.MinLeasedTimestampGetterImpl;
import com.palantir.lock.client.timestampleases.NamespacedTimestampLeaseService;
import com.palantir.lock.client.timestampleases.NamespacedTimestampLeaseServiceImpl;
import com.palantir.lock.client.timestampleases.TimestampLeaseAcquirer;
import com.palantir.lock.client.timestampleases.TimestampLeaseAcquirerImpl;
import com.palantir.lock.v2.ClientLockingOptions;
import com.palantir.lock.v2.DefaultNamespacedTimelockRpcClient;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.NamespacedTimelockRpcClient;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionResponse;
import com.palantir.lock.v2.TimelockRpcClient;
import com.palantir.lock.v2.TimelockService;
import com.palantir.lock.v2.TimestampLeaseResults;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.lock.v2.WaitForLocksResponse;
import com.palantir.lock.watch.LockWatchCache;
import com.palantir.lock.watch.LockWatchCacheImpl;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.timestamp.TimestampRange;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public final class RemoteTimelockServiceAdapter implements TimelockService, AutoCloseable {
    private static final SafeLogger log = SafeLoggerFactory.get(RemoteTimelockServiceAdapter.class);

    private final NamespacedTimelockRpcClient rpcClient;
    private final NamespacedConjureTimelockService conjureTimelockService;
    private final LockLeaseService lockLeaseService;
    private final TransactionStarter transactionStarter;
    private final CommitTimestampGetter commitTimestampGetter;
    private final TimestampLeaseAcquirer timestampLeaseAcquirer;
    private final MinLeasedTimestampGetter minLeasedTimestampGetter;

    private RemoteTimelockServiceAdapter(
            NamespacedTimelockRpcClient rpcClient,
            NamespacedConjureTimelockService conjureClient,
            LockLeaseService lockLeaseService,
            RequestBatchersFactory batcherFactory,
            TimestampLeaseAcquirer timestampLeaseAcquirer,
            MinLeasedTimestampGetter minLeasedTimestampGetter) {
        this.rpcClient = rpcClient;
        this.conjureTimelockService = conjureClient;
        this.lockLeaseService = lockLeaseService;
        this.transactionStarter = TransactionStarter.create(lockLeaseService, batcherFactory);
        this.commitTimestampGetter = batcherFactory.createBatchingCommitTimestampGetter(lockLeaseService);
        this.timestampLeaseAcquirer = timestampLeaseAcquirer;
        this.minLeasedTimestampGetter = minLeasedTimestampGetter;
    }

    public RemoteTimelockServiceAdapter(
            NamespacedTimelockRpcClient rpcClient,
            NamespacedConjureTimelockService conjureTimelockService,
            LockLeaseService lockLeaseService,
            TransactionStarter transactionStarter,
            CommitTimestampGetter commitTimestampGetter,
            TimestampLeaseAcquirer timestampLeaseAcquirer,
            MinLeasedTimestampGetter minLeasedTimestampGetter) {
        this.rpcClient = rpcClient;
        this.conjureTimelockService = conjureTimelockService;
        this.lockLeaseService = lockLeaseService;
        this.transactionStarter = transactionStarter;
        this.commitTimestampGetter = commitTimestampGetter;
        this.timestampLeaseAcquirer = timestampLeaseAcquirer;
        this.minLeasedTimestampGetter = minLeasedTimestampGetter;
    }

    public static TimelockService create(
            Namespace namespace,
            NamespacedTimelockRpcClient rpcClient,
            NamespacedConjureTimelockService conjureClient,
            LockWatchCache lockWatchCache,
            LockLeaseService lockLeaseService,
            TimestampLeaseAcquirer timestampLeaseAcquirer,
            MinLeasedTimestampGetter minLeasedTimestampGetter) {
        return new RemoteTimelockServiceAdapter(
                rpcClient,
                conjureClient,
                lockLeaseService,
                RequestBatchersFactory.create(lockWatchCache, namespace, Optional.empty()),
                timestampLeaseAcquirer,
                minLeasedTimestampGetter);
    }

    public static RemoteTimelockServiceAdapter create(
            NamespacedTimelockRpcClient rpcClient,
            NamespacedConjureTimelockService conjureClient,
            RequestBatchersFactory batcherFactory,
            LockLeaseService lockLeaseService,
            TimestampLeaseAcquirer timestampLeaseAcquirer,
            MinLeasedTimestampGetter minLeasedTimestampGetter) {
        return new RemoteTimelockServiceAdapter(
                rpcClient,
                conjureClient,
                lockLeaseService,
                batcherFactory,
                timestampLeaseAcquirer,
                minLeasedTimestampGetter);
    }

    public static RemoteTimelockServiceAdapter create(
            Namespace namespace,
            TimelockRpcClient baseRpcClient,
            ConjureTimelockService baseConjureClient,
            MultiClientConjureTimelockServiceBlocking multiClientConjureClient) {
        NamespacedTimestampLeaseService timestampLeaseService = new NamespacedTimestampLeaseServiceImpl(
                namespace, new AuthenticatedInternalMultiClientConjureTimelockService(multiClientConjureClient));

        NamespacedConjureTimelockService conjureClient =
                new NamespacedConjureTimelockServiceImpl(baseConjureClient, namespace.get());

        LockTokenUnlocker unlocker = new LegacyLockTokenUnlocker(conjureClient);

        return RemoteTimelockServiceAdapter.create(
                new DefaultNamespacedTimelockRpcClient(baseRpcClient, namespace.get()),
                conjureClient,
                RequestBatchersFactory.create(LockWatchCacheImpl.noOp(), namespace, Optional.empty()),
                LockLeaseService.create(conjureClient, new LegacyLeaderTimeGetter(conjureClient), unlocker),
                TimestampLeaseAcquirerImpl.create(timestampLeaseService, unlocker),
                new MinLeasedTimestampGetterImpl(timestampLeaseService));
    }

    @Override
    public long getFreshTimestamp() {
        return conjureTimelockService.getFreshTimestamp().get();
    }

    @Override
    public long getCommitTimestamp(long startTs, LockToken commitLocksToken) {
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
    public TimestampLeaseResults acquireTimestampLeases(Map<TimestampLeaseName, Integer> requests) {
        return timestampLeaseAcquirer.acquireNamedTimestampLeases(requests);
    }

    @Override
    public Map<TimestampLeaseName, Long> getMinLeasedTimestamps(Set<TimestampLeaseName> timestampNames) {
        return minLeasedTimestampGetter.getMinLeasedTimestamps(timestampNames);
    }

    @Override
    public void close() {
        transactionStarter.close();
        commitTimestampGetter.close();
        lockLeaseService.close();
        timestampLeaseAcquirer.close();
        minLeasedTimestampGetter.close();
    }
}
