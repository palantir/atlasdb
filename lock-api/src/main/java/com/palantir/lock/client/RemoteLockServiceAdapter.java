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

import com.palantir.lock.HeldLocksGrant;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockResponse;
import com.palantir.lock.LockRpcClient;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.LockService;
import com.palantir.lock.NamespaceAgnosticLockRpcClient;
import com.palantir.lock.SimpleHeldLocksToken;
import java.math.BigInteger;
import java.util.Set;
import javax.annotation.Nullable;

public class RemoteLockServiceAdapter implements LockService {
    private final NamespaceAgnosticLockRpcClient namespaceAgnosticLockRpcClient;

    public RemoteLockServiceAdapter(NamespaceAgnosticLockRpcClient namespaceAgnosticLockRpcClient) {
        this.namespaceAgnosticLockRpcClient = namespaceAgnosticLockRpcClient;
    }

    public static LockService create(LockRpcClient lockRpcClient, String namespace) {
        NamespaceAgnosticLockRpcClient namespaceAgnosticClient
                = new NamespaceAgnosticLockClientAdaptor(namespace, lockRpcClient);
        return new RemoteLockServiceAdapter(namespaceAgnosticClient);
    }

    @Override
    public LockResponse lockWithFullLockResponse(LockClient client, LockRequest request) throws InterruptedException {
        return namespaceAgnosticLockRpcClient.lockWithFullLockResponse(client, request).orElse(null);
    }

    @Override
    public boolean unlock(HeldLocksToken token) {
        return namespaceAgnosticLockRpcClient.unlock(token);
    }

    @Override
    public boolean unlock(LockRefreshToken token) {
        return namespaceAgnosticLockRpcClient.unlock(token);
    }

    @Override
    public boolean unlockSimple(SimpleHeldLocksToken token) {
        return namespaceAgnosticLockRpcClient.unlockSimple(token);
    }

    @Override
    public boolean unlockAndFreeze(HeldLocksToken token) {
        return namespaceAgnosticLockRpcClient.unlockAndFreeze(token);
    }

    @Override
    public Set<HeldLocksToken> getTokens(LockClient client) {
        return namespaceAgnosticLockRpcClient.getTokens(client);
    }

    @Override
    public Set<HeldLocksToken> refreshTokens(Iterable<HeldLocksToken> tokens) {
        return namespaceAgnosticLockRpcClient.refreshTokens(tokens);
    }

    @Nullable
    @Override
    public HeldLocksGrant refreshGrant(HeldLocksGrant grant) {
        return namespaceAgnosticLockRpcClient.refreshGrant(grant).orElse(null);
    }

    @Nullable
    @Override
    public HeldLocksGrant refreshGrant(BigInteger grantId) {
        return namespaceAgnosticLockRpcClient.refreshGrant(grantId).orElse(null);
    }

    @Override
    public HeldLocksGrant convertToGrant(HeldLocksToken token) {
        return namespaceAgnosticLockRpcClient.convertToGrant(token);
    }

    @Override
    public HeldLocksToken useGrant(LockClient client, HeldLocksGrant grant) {
        return namespaceAgnosticLockRpcClient.useGrant(client, grant);
    }

    @Override
    public HeldLocksToken useGrant(LockClient client, BigInteger grantId) {
        return namespaceAgnosticLockRpcClient.useGrant(client, grantId);
    }

    @Nullable
    @Override
    public Long getMinLockedInVersionId() {
        return namespaceAgnosticLockRpcClient.getMinLockedInVersionId().orElse(null);
    }

    @Override
    public Long getMinLockedInVersionId(LockClient client) {
        return namespaceAgnosticLockRpcClient.getMinLockedInVersionId(client).orElse(null);
    }

    @Nullable
    @Override
    public Long getMinLockedInVersionId(String client) {
        return namespaceAgnosticLockRpcClient.getMinLockedInVersionId(client).orElse(null);
    }

    @Override
    public LockServerOptions getLockServerOptions() {
        return namespaceAgnosticLockRpcClient.getLockServerOptions();
    }

    @Nullable
    @Override
    public LockRefreshToken lock(String client, LockRequest request) throws InterruptedException {
        return namespaceAgnosticLockRpcClient.lock(client, request).orElse(null);
    }

    @Override
    public HeldLocksToken lockAndGetHeldLocks(String client, LockRequest request) throws InterruptedException {
        return namespaceAgnosticLockRpcClient.lockAndGetHeldLocks(client, request).orElse(null);
    }

    @Override
    public Set<LockRefreshToken> refreshLockRefreshTokens(Iterable<LockRefreshToken> tokens) {
        return namespaceAgnosticLockRpcClient.refreshLockRefreshTokens(tokens);
    }

    @Override
    public long currentTimeMillis() {
        return namespaceAgnosticLockRpcClient.currentTimeMillis();
    }

    @Override
    public void logCurrentState() {
        namespaceAgnosticLockRpcClient.logCurrentState();
    }
}
