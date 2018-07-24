/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.lock;

import java.math.BigInteger;
import java.util.Set;
import java.util.function.Supplier;

import javax.annotation.Nullable;

import com.palantir.tokens.auth.AuthHeader;

public class AuthDecoratedLockService implements LockService {
    private final AuthedLockService authedLockService;
    private final Supplier<AuthHeader> authHeaderSupplier;

    public AuthDecoratedLockService(AuthedLockService authedLockService,
            Supplier<AuthHeader> authHeaderSupplier) {
        this.authedLockService = authedLockService;
        this.authHeaderSupplier = authHeaderSupplier;
    }

    @Override
    public LockResponse lockWithFullLockResponse(LockClient client, LockRequest request) throws InterruptedException {
        return authedLockService.lockWithFullLockResponse(authHeaderSupplier.get(), client, request);
    }

    @Override
    public boolean unlock(HeldLocksToken token) {
        return authedLockService.unlock(authHeaderSupplier.get(), token);
    }

    @Override
    public boolean unlock(LockRefreshToken token) {
        return authedLockService.unlock(authHeaderSupplier.get(), token);
    }

    @Override
    public boolean unlockSimple(SimpleHeldLocksToken token) {
        return authedLockService.unlockSimple(authHeaderSupplier.get(), token);
    }

    @Override
    public boolean unlockAndFreeze(HeldLocksToken token) {
        return authedLockService.unlockAndFreeze(authHeaderSupplier.get(), token);
    }

    @Override
    public Set<HeldLocksToken> getTokens(LockClient client) {
        return authedLockService.getTokens(authHeaderSupplier.get(), client);
    }

    @Override
    public Set<HeldLocksToken> refreshTokens(Iterable<HeldLocksToken> tokens) {
        return authedLockService.refreshTokens(authHeaderSupplier.get(), tokens);
    }

    @Nullable
    @Override
    public HeldLocksGrant refreshGrant(HeldLocksGrant grant) {
        return authedLockService.refreshGrant(authHeaderSupplier.get(), grant);
    }

    @Nullable
    @Override
    public HeldLocksGrant refreshGrant(BigInteger grantId) {
        return authedLockService.refreshGrant(authHeaderSupplier.get(), grantId);
    }

    @Override
    public HeldLocksGrant convertToGrant(HeldLocksToken token) {
        return authedLockService.convertToGrant(authHeaderSupplier.get(), token);
    }

    @Override
    public HeldLocksToken useGrant(LockClient client, HeldLocksGrant grant) {
        return authedLockService.useGrant(authHeaderSupplier.get(), client, grant);
    }

    @Override
    public HeldLocksToken useGrant(LockClient client, BigInteger grantId) {
        return authedLockService.useGrant(authHeaderSupplier.get(), client, grantId);
    }

    @Nullable
    @Override
    public Long getMinLockedInVersionId() {
        return authedLockService.getMinLockedInVersionId(authHeaderSupplier.get());
    }

    @Override
    public Long getMinLockedInVersionId(LockClient client) {
        return authedLockService.getMinLockedInVersionId(authHeaderSupplier.get(), client);
    }

    @Nullable
    @Override
    public Long getMinLockedInVersionId(String client) {
        return authedLockService.getMinLockedInVersionId(authHeaderSupplier.get(), client);
    }

    @Override
    public LockServerOptions getLockServerOptions() {
        return authedLockService.getLockServerOptions(authHeaderSupplier.get());
    }

    @Nullable
    @Override
    public LockRefreshToken lock(String client, LockRequest request) throws InterruptedException {
        return authedLockService.lock(authHeaderSupplier.get(), client, request);
    }

    @Override
    public HeldLocksToken lockAndGetHeldLocks(String client, LockRequest request) throws InterruptedException {
        return authedLockService.lockAndGetHeldLocks(authHeaderSupplier.get(), client, request);
    }

    @Override
    public Set<LockRefreshToken> refreshLockRefreshTokens(Iterable<LockRefreshToken> tokens) {
        return authedLockService.refreshLockRefreshTokens(authHeaderSupplier.get(), tokens);
    }

    @Override
    public long currentTimeMillis() {
        return authedLockService.currentTimeMillis(authHeaderSupplier.get());
    }

    @Override
    public void logCurrentState() {
        authedLockService.logCurrentState(authHeaderSupplier.get());
    }

}
