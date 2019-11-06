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

package com.palantir.atlasdb.debug;

import java.util.Optional;

import com.palantir.lock.client.IdentifiedLockRequest;
import com.palantir.lock.v2.AutoDelegate_TimelockRpcClient;
import com.palantir.lock.v2.LockResponseV2;
import com.palantir.lock.v2.StartTransactionRequestV4;
import com.palantir.lock.v2.StartTransactionResponseV4;
import com.palantir.lock.v2.TimelockRpcClient;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.lock.v2.WaitForLocksResponse;

/**
 * TODO(fdesouza): Remove this once PDS-95791 is resolved
 */
@Deprecated
public class LockDiagnosticTimelockRpcClient implements AutoDelegate_TimelockRpcClient {

    private final TimelockRpcClient delegate;
    private final ClientLockDiagnosticCollector lockDiagnosticCollector;

    public LockDiagnosticTimelockRpcClient(
            TimelockRpcClient delegate,
            ClientLockDiagnosticCollector lockDiagnosticCollector) {
        this.delegate = delegate;
        this.lockDiagnosticCollector = lockDiagnosticCollector;
    }

    @Override
    public TimelockRpcClient delegate() {
        return delegate;
    }

    @Override
    public StartTransactionResponseV4 startTransactions(String namespace, StartTransactionRequestV4 request) {
        StartTransactionResponseV4 response = delegate().startTransactions(namespace, request);
        lockDiagnosticCollector.collect(
                response.timestamps().stream(),
                response.immutableTimestamp().getImmutableTimestamp(),
                request.requestId());
        return response;
    }

    @Override
    public WaitForLocksResponse waitForLocks(String namespace, WaitForLocksRequest request) {
        request.getClientDescription()
                .flatMap(LockDiagnosticTimelockRpcClient::tryParseStartTimestamp)
                .ifPresent(startTimestamp -> lockDiagnosticCollector.collect(
                        startTimestamp, request.getRequestId(), request.getLockDescriptors()));
        return delegate().waitForLocks(namespace, request);
    }

    @Override
    public LockResponseV2 lock(String namespace, IdentifiedLockRequest request) {
        request.getClientDescription()
                .flatMap(LockDiagnosticTimelockRpcClient::tryParseStartTimestamp)
                .ifPresent(startTimestamp -> lockDiagnosticCollector.collect(
                        startTimestamp, request.getRequestId(), request.getLockDescriptors()));
        return delegate().lock(namespace, request);
    }

    private static Optional<Long> tryParseStartTimestamp(String description) {
        try {
            return Optional.of(Long.parseLong(description));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }
}
