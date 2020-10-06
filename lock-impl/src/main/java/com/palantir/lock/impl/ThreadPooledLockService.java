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
package com.palantir.lock.impl;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Set;
import java.util.concurrent.Semaphore;

import javax.annotation.Nullable;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.lock.CloseableLockService;
import com.palantir.lock.HeldLocksGrant;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockResponse;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.LockService;
import com.palantir.lock.SimpleHeldLocksToken;

// TODO: Really, this should sit in front of AwaitingLeadershipProxy
public class ThreadPooledLockService implements AsyncCloseableLockService {
    private final ThreadPooledWrapper<LockService> wrapper;
    private final CloseableLockService delegate;

    public ThreadPooledLockService(CloseableLockService delegate, int localThreadPoolSize, Semaphore sharedThreadPool) {
        this.delegate = delegate;
        wrapper = new ThreadPooledWrapper<>(delegate, localThreadPoolSize, sharedThreadPool);
    }

    @Nullable
    @Override
    public ListenableFuture<LockRefreshToken> lock(String client, LockRequest request) {
        return toFuture(() -> wrapper.applyWithPermit(lockService -> lockService.lock(client, request)));
    }

    @Override
    public ListenableFuture<HeldLocksToken> lockAndGetHeldLocks(String client, LockRequest request) {
        return toFuture(() -> wrapper.applyWithPermit(lockService -> lockService.lockAndGetHeldLocks(client, request)));
    }

    @Override
    public ListenableFuture<Boolean> unlock(LockRefreshToken token) {
        return toFuture(() -> delegate.unlock(token));
    }

    @Override
    public ListenableFuture<Set<LockRefreshToken>> refreshLockRefreshTokens(Iterable<LockRefreshToken> tokens) {
        return toFuture(() -> delegate.refreshLockRefreshTokens(tokens));
    }

    @Nullable
    @Override
    public ListenableFuture<Long> getMinLockedInVersionId(String client) {
        return toFuture(() -> delegate.getMinLockedInVersionId(client));
    }

    @Override
    public ListenableFuture<LockResponse> lockWithFullLockResponse(LockClient client, LockRequest request) {
        return toFuture(
                () -> wrapper.applyWithPermit(lockService -> lockService.lockWithFullLockResponse(client, request)));
    }

    @Override
    public ListenableFuture<Boolean> unlock(HeldLocksToken token) {
        return toFuture(() -> delegate.unlock(token));
    }

    @Override
    public ListenableFuture<Boolean> unlockSimple(SimpleHeldLocksToken token) {
        return toFuture(() -> delegate.unlockSimple(token));
    }

    @Override
    public ListenableFuture<Boolean> unlockAndFreeze(HeldLocksToken token) {
        return toFuture(() -> delegate.unlockAndFreeze(token));
    }

    @Override
    public ListenableFuture<Set<HeldLocksToken>> getTokens(LockClient client) {
        return toFuture(() -> delegate.getTokens(client));
    }

    @Override
    public ListenableFuture<Set<HeldLocksToken>> refreshTokens(Iterable<HeldLocksToken> tokens) {
        return toFuture(() -> delegate.refreshTokens(tokens));
    }

    @Nullable
    @Override
    public ListenableFuture<HeldLocksGrant> refreshGrant(HeldLocksGrant grant) {
        return toFuture(() -> delegate.refreshGrant(grant));
    }

    @Nullable
    @Override
    public ListenableFuture<HeldLocksGrant> refreshGrant(BigInteger grantId) {
        return toFuture(() -> delegate.refreshGrant(grantId));
    }

    @Override
    public ListenableFuture<HeldLocksGrant> convertToGrant(HeldLocksToken token) {
        return toFuture(() -> delegate.convertToGrant(token));
    }

    @Override
    public ListenableFuture<HeldLocksToken> useGrant(LockClient client, HeldLocksGrant grant) {
        return toFuture(() -> delegate.useGrant(client, grant));
    }

    @Override
    public ListenableFuture<HeldLocksToken> useGrant(LockClient client, BigInteger grantId) {
        return toFuture(() -> delegate.useGrant(client, grantId));
    }

    @Nullable
    @Override
    public ListenableFuture<Long> getMinLockedInVersionId() {
        return toFuture(delegate::getMinLockedInVersionId);
    }

    @Override
    public ListenableFuture<Long> getMinLockedInVersionId(LockClient client) {
        return toFuture(() -> delegate.getMinLockedInVersionId(client));
    }

    @Override
    public ListenableFuture<LockServerOptions> getLockServerOptions() {
        return toFuture(delegate::getLockServerOptions);
    }

    @Override
    public ListenableFuture<Long> currentTimeMillis() {
        return toFuture(delegate::currentTimeMillis);
    }

    @Override
    public ListenableFuture<Void> logCurrentState() {
        return toFuture(() -> {
            delegate.logCurrentState();
            return null;
        });
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    private <V> ListenableFuture<V> toFuture(CheckedSupplier<V, InterruptedException> supplier) {
        try {
            return Futures.immediateFuture(supplier.get());
        } catch (InterruptedException e) {
            return Futures.immediateFailedFuture(e);
        }
    }

    private interface CheckedSupplier<T, K extends Exception> {
        T get() throws K;
    }
}
