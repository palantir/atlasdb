/**
 * Copyright 2017 Palantir Technologies
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
package com.palantir.lock.impl;

import java.math.BigInteger;
import java.util.Set;

import javax.annotation.Nullable;

import com.palantir.lock.HeldLocksGrant;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockResponse;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.LockService;
import com.palantir.lock.SimpleHeldLocksToken;

public class RateLimitingLockService implements LockService{
    private static final ThreadCountLimiter globalLimiter = new ThreadCountLimiter(0);

    private LockService delegate;
    private ThreadCountLimiter limiter;

    private RateLimitingLockService() {
        // hidden
    }

    public static RateLimitingLockService create(LockService delegate, int maxServerThreads, int numClients) {
        int threadPoolSize = maxServerThreads - 15;
        int localLimiterSize = threadPoolSize / numClients / 2;
        int globalLimiterSize = threadPoolSize - localLimiterSize * numClients;

        RateLimitingLockService service = new RateLimitingLockService();
        service.delegate = delegate;
        service.limiter = new ThreadCountLimiter(localLimiterSize);
        globalLimiter.set(globalLimiterSize);
        return service;
    }


    @Nullable
    @Override
    public LockRefreshToken lock(String client, LockRequest request) throws InterruptedException {
        if (limiter.acquire()) {
            LockRefreshToken token = delegate.lock(client, request);
            limiter.release();
            return token;
        } else if (globalLimiter.acquire()) {
            LockRefreshToken token = delegate.lock(client, request);
            globalLimiter.release();
            return token;
        } else {
            throw new TooManyRequestsException();
        }
    }

    @Override
    public HeldLocksToken lockAndGetHeldLocks(String client, LockRequest request) throws InterruptedException {
        return null;
    }

    @Override
    public boolean unlock(LockRefreshToken token) {
        return delegate.unlock(token);
    }

    @Override
    public Set<LockRefreshToken> refreshLockRefreshTokens(Iterable<LockRefreshToken> tokens) {
        if (limiter.acquire()) {
            Set<LockRefreshToken> result = delegate.refreshLockRefreshTokens(tokens);
            limiter.release();
            return result;
        } else if (globalLimiter.acquire()) {
            Set<LockRefreshToken> result = delegate.refreshLockRefreshTokens(tokens);
            globalLimiter.release();
            return result;
        } else {
            throw new TooManyRequestsException();
        }
    }

    @Nullable
    @Override
    public Long getMinLockedInVersionId(String client) {
        return null;
    }

    @Override
    public LockResponse lockWithFullLockResponse(LockClient client, LockRequest request) throws InterruptedException {
        return null;
    }

    @Override
    public boolean unlock(HeldLocksToken token) {
        return delegate.unlock(token);
    }

    @Override
    public boolean unlockSimple(SimpleHeldLocksToken token) {
        return false;
    }

    @Override
    public boolean unlockAndFreeze(HeldLocksToken token) {
        return false;
    }

    @Override
    public Set<HeldLocksToken> getTokens(LockClient client) {
        return null;
    }

    @Override
    public Set<HeldLocksToken> refreshTokens(Iterable<HeldLocksToken> tokens) {
        return null;
    }

    @Nullable
    @Override
    public HeldLocksGrant refreshGrant(HeldLocksGrant grant) {
        return null;
    }

    @Nullable
    @Override
    public HeldLocksGrant refreshGrant(BigInteger grantId) {
        return null;
    }

    @Override
    public HeldLocksGrant convertToGrant(HeldLocksToken token) {
        return null;
    }

    @Override
    public HeldLocksToken useGrant(LockClient client, HeldLocksGrant grant) {
        return null;
    }

    @Override
    public HeldLocksToken useGrant(LockClient client, BigInteger grantId) {
        return null;
    }

    @Nullable
    @Override
    public Long getMinLockedInVersionId() {
        return null;
    }

    @Override
    public Long getMinLockedInVersionId(LockClient client) {
        return null;
    }

    @Override
    public LockServerOptions getLockServerOptions() {
        return null;
    }

    @Override
    public long currentTimeMillis() {
        return 0;
    }

    @Override
    public void logCurrentState() {

    }
}
