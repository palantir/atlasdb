/*
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

import java.util.Set;
import java.util.concurrent.Semaphore;

import javax.annotation.Nullable;

import com.palantir.common.base.FunctionCheckedException;
import com.palantir.lock.ForwardingLockService;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockResponse;
import com.palantir.lock.LockService;

public class RateLimitingLockService extends ForwardingLockService {
    private static final Semaphore globalLimiter = new Semaphore(-1);

    final LockService delegate;
    final Semaphore limiter;

    private RateLimitingLockService(LockService delegate, int localLimiterSize) {
        this.delegate = delegate;
        this.limiter = new Semaphore(localLimiterSize);
    }

    public static RateLimitingLockService create(LockService delegate, int availableThreads, int numClients) {
        // TODO availableThreads is non-negative due to dropwizard asserts, but still consider making this look better
        numClients = Math.max(numClients, 1);

        int localLimiterSize = availableThreads / numClients / 2;
        int globalLimiterSize = availableThreads - localLimiterSize * numClients;

        // TODO a more robust solution is needed for live reloading
        if (globalLimiter.availablePermits() == -1){
            globalLimiter.release(globalLimiterSize + 1);
        }
        return new RateLimitingLockService(delegate, localLimiterSize);
    }


    @Override
    protected LockService delegate() {
        return delegate;
    }

    @Nullable
    @Override
    public LockRefreshToken lock(String client, LockRequest request) throws InterruptedException {
        return applyWithPermit(lockService -> lockService.lock(client, request));
    }

    @Override
    public HeldLocksToken lockAndGetHeldLocks(String client, LockRequest request)
            throws InterruptedException {
        return applyWithPermit(lockService -> lockService.lockAndGetHeldLocks(client, request));
    }

    @Override
    public LockResponse lockWithFullLockResponse(LockClient client, LockRequest request) throws InterruptedException {
        return applyWithPermit(lockService -> lockService.lockWithFullLockResponse(client, request));
    }

    @Override
    public Set<HeldLocksToken> refreshTokens(Iterable<HeldLocksToken> tokens) {
        return applyWithPermit(lockService -> lockService.refreshTokens(tokens));
    }

    @Override
    public Set<LockRefreshToken> refreshLockRefreshTokens(Iterable<LockRefreshToken> tokens) {
        return applyWithPermit(lockService -> lockService.refreshLockRefreshTokens(tokens));
    }

    private <T, K extends Exception> T applyWithPermit(FunctionCheckedException<LockService, T, K> function) throws K {
        if (limiter.tryAcquire()) {
            return applyAndRelease(limiter, function);
        } else if (globalLimiter.tryAcquire()) {
            return applyAndRelease(globalLimiter, function);
        }
        throw new TooManyRequestsException();
    }

    private <T, K extends Exception> T applyAndRelease(Semaphore semaphore,
            FunctionCheckedException<LockService, T, K> function) throws K {
        try {
            return function.apply(delegate);
        } finally {
            semaphore.release();
        }
    }
}
