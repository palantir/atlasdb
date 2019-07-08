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

package com.palantir.lock;

import java.util.Set;

import javax.ws.rs.PathParam;

import com.palantir.logsafe.Safe;

public class RemoteLockServiceAdapter implements RemoteLockService {
    private final RemoteLockRpcClient remoteLockRpcClient;

    public RemoteLockServiceAdapter(RemoteLockRpcClient remoteLockRpcClient) {
        this.remoteLockRpcClient = remoteLockRpcClient;
    }

    @Override
    public LockRefreshToken lock(@Safe @PathParam("client") String client, LockRequest request)
            throws InterruptedException {
        return remoteLockRpcClient.lock(client, request).orElse(null);
    }

    @Override
    public HeldLocksToken lockAndGetHeldLocks(@Safe @PathParam("client") String client, LockRequest request)
            throws InterruptedException {
        return remoteLockRpcClient.lockAndGetHeldLocks(client, request).orElse(null);
    }

    @Override
    public boolean unlock(LockRefreshToken token) {
        return remoteLockRpcClient.unlock(token);
    }

    @Override
    public Set<LockRefreshToken> refreshLockRefreshTokens(Iterable<LockRefreshToken> tokens) {
        return remoteLockRpcClient.refreshLockRefreshTokens(tokens);
    }

    @Override
    public Long getMinLockedInVersionId(@Safe @PathParam("client") String client) {
        return remoteLockRpcClient.getMinLockedInVersionId(client).orElse(null);
    }

    @Override
    public long currentTimeMillis() {
        return remoteLockRpcClient.currentTimeMillis();
    }

    @Override
    public void logCurrentState() {
        remoteLockRpcClient.logCurrentState();
    }
}
