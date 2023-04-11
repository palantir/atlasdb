/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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
import com.palantir.atlasdb.debug.LockDiagnosticInfo;
import com.palantir.atlasdb.timelock.lock.LockLog;
import com.palantir.conjure.java.undertow.annotations.Handle;
import com.palantir.conjure.java.undertow.annotations.HttpMethod;
import com.palantir.lock.client.IdentifiedLockRequest;
import com.palantir.lock.v2.IdentifiedTimeLockRequest;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockResponseV2;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.RefreshLockResponseV2;
import com.palantir.lock.v2.StartAtlasDbTransactionResponse;
import com.palantir.lock.v2.StartAtlasDbTransactionResponseV3;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionRequest;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionResponse;
import com.palantir.lock.v2.StartTransactionRequestV4;
import com.palantir.lock.v2.StartTransactionResponseV4;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.lock.v2.WaitForLocksResponse;
import com.palantir.logsafe.Safe;
import com.palantir.timestamp.TimestampRange;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

public final class UndertowAsyncTimelockResource {
    private final TimelockNamespaces namespaces;

    public UndertowAsyncTimelockResource(TimelockNamespaces namespaces) {
        this.namespaces = namespaces;
    }

    @Handle(method = HttpMethod.POST, path = "/{namespace}/timelock/fresh-timestamp")
    public ListenableFuture<Long> getFreshTimestamp(@Safe @Handle.PathParam String namespace) {
        return getTimelockService(namespace).getFreshTimestampAsync();
    }

    @Handle(method = HttpMethod.POST, path = "/{namespace}/timelock/fresh-timestamps")
    public ListenableFuture<TimestampRange> getFreshTimestamps(
            @Safe @Handle.PathParam String namespace, @Safe @Handle.QueryParam(value = "number") int numTimestampsRequested) {
        return getTimelockService(namespace).getFreshTimestampsAsync(numTimestampsRequested);
    }

    @Handle(method = HttpMethod.POST, path = "/{namespace}/timelock/lock-immutable-timestamp")
    public LockImmutableTimestampResponse lockImmutableTimestamp(@Safe @Handle.PathParam String namespace, @Handle.Body IdentifiedTimeLockRequest request) {
        return getTimelockService(namespace).lockImmutableTimestamp(request);
    }

    @Handle(method = HttpMethod.POST, path = "/{namespace}/timelock/start-atlasdb-transaction")
    public StartAtlasDbTransactionResponse deprecatedStartTransactionV1(@Safe @Handle.PathParam String namespace, @Handle.Body IdentifiedTimeLockRequest request) {
        return getTimelockService(namespace).deprecatedStartTransaction(request);
    }

    @Handle(method = HttpMethod.POST, path = "/{namespace}/timelock/start-identified-atlasdb-transaction")
    public StartIdentifiedAtlasDbTransactionResponse deprecatedStartTransactionV2(
            @Safe @Handle.PathParam String namespace, @Handle.Body   StartIdentifiedAtlasDbTransactionRequest request) {
        return getTimelockService(namespace).startTransaction(request).toStartTransactionResponse();
    }

    @Handle(method = HttpMethod.POST, path = "/{namespace}/timelock/start-atlasdb-transaction-v3")
    public StartAtlasDbTransactionResponseV3 deprecatedStartTransactionV3(
            @Safe @Handle.PathParam String namespace, @Handle.Body             StartIdentifiedAtlasDbTransactionRequest request) {
        return getTimelockService(namespace).startTransaction(request);
    }

    @Handle(method = HttpMethod.POST, path = "/{namespace}/timelock/start-atlasdb-transaction-v4")
    public ListenableFuture<StartTransactionResponseV4> startTransactions(@Safe @Handle.PathParam String namespace, @Handle.Body  StartTransactionRequestV4 request) {
        return getTimelockService(namespace).startTransactionsAsync(request);
    }

    @Handle(method = HttpMethod.POST, path = "/{namespace}/timelock/immutable-timestamp")
    public long getImmutableTimestamp(@Safe @Handle.PathParam String namespace) {
        return getTimelockService(namespace).getImmutableTimestamp();
    }

    @Handle(method = HttpMethod.POST, path = "/{namespace}/timelock/lock")
    public ListenableFuture<LockResponse> deprecatedLock(@Safe @Handle.PathParam String namespace, @Handle.Body IdentifiedLockRequest request) {
        return getTimelockService(namespace).deprecatedLock(request);
    }

    @Handle(method = HttpMethod.POST, path = "/{namespace}/timelock/lock-v2")
    public ListenableFuture<LockResponseV2> lock(@Safe @Handle.PathParam String namespace, @Handle.Body  IdentifiedLockRequest request) {
        return getTimelockService(namespace).lock(request);
    }

    @Handle(method = HttpMethod.POST, path = "/{namespace}/timelock/await-locks")
    public ListenableFuture<WaitForLocksResponse> waitForLocks(@Safe @Handle.PathParam String namespace, @Handle.Body   WaitForLocksRequest request) {
        return getTimelockService(namespace).waitForLocks(request);
    }

    @Handle(method = HttpMethod.POST, path = "/{namespace}/timelock/refresh-locks")
    public ListenableFuture<Set<LockToken>> deprecatedRefreshLockLeases(@Safe @Handle.PathParam String namespace, @Handle.Body   Set<LockToken> tokens) {
        return getTimelockService(namespace).deprecatedRefreshLockLeases(tokens);
    }

    @Handle(method = HttpMethod.POST, path = "/{namespace}/timelock/refresh-locks-v2")
    public ListenableFuture<RefreshLockResponseV2> refreshLockLeases(@Safe @Handle.PathParam String namespace, @Handle.Body   Set<LockToken> tokens) {
        return getTimelockService(namespace).refreshLockLeases(tokens);
    }

    @Handle(method = HttpMethod.GET, path = "/{namespace}/timelock/leader-time")
    public ListenableFuture<LeaderTime> getLeaderTime(@Safe @Handle.PathParam String namespace) {
        return getTimelockService(namespace).leaderTime();
    }

    @Handle(method = HttpMethod.POST, path = "/{namespace}/timelock/unlock")
    public ListenableFuture<Set<LockToken>> unlock(@Safe @Handle.PathParam String namespace, @Handle.Body  Set<LockToken> tokens) {
        return getTimelockService(namespace).unlock(tokens);
    }

    @Handle(method = HttpMethod.POST, path = "/{namespace}/timelock/current-time-millis")
    public long currentTimeMillis(@Safe @Handle.PathParam String namespace) {
        return getTimelockService(namespace).currentTimeMillis();
    }

    // TODO(jkong): Remove this once PDS-95791 is resolved.
    @Handle(method = HttpMethod.POST, path = "/{namespace}/timelock/do-not-use-without-explicit-atlasdb-authorisation/lock-diagnostic-config")
    public Optional<LockDiagnosticInfo> getEnhancedLockDiagnosticInfo(@Safe @Handle.PathParam String namespace, @Handle.Body  Set<UUID> requestIds) {
        return namespaces.get(namespace).getLockLog().getAndLogLockDiagnosticInfo(requestIds);
    }

    private AsyncTimelockService getTimelockService(String namespace) {
        return namespaces.get(namespace).getTimelockService();
    }
}
