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

import org.immutables.value.Value;
import org.slf4j.LoggerFactory;

import com.palantir.common.base.FunctionCheckedException;
import com.palantir.lock.ForwardingLockService;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockResponse;
import com.palantir.lock.LockService;

public class RateLimitingLockService extends ForwardingLockService {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(LockServiceImpl.class);
    private static final Semaphore globalLimiter = new Semaphore(-1);

    final LockService delegate;
    final Semaphore localLimiter;


    private RateLimitingLockService(LockService delegate, int localLimiterSize) {
        this.delegate = delegate;
        this.localLimiter = new Semaphore(localLimiterSize);
    }

    public static LockService create(LockService delegate, RateLimitingConfiguration configuration) {
        if (configuration.useRateLimiting() == false) {
            return delegate;
        }

        int numClients = configuration.numClients();
        if (numClients == 0) {
            log.warn("TimeLockServerConfiguration specifies 0 clients. Rate limiter will default to 1 client.");
            numClients = 1;
        }
        if (configuration.availableThreads() < 1) {
            log.warn("TimeLockServerConfiguration specifies less than 1 available server thread. Rate limiting will be "
                    + "disabled.");
            return delegate;
        }
        int localLimiterSize = configuration.availableThreads() / numClients / 2;
        int globalLimiterSize = configuration.availableThreads() - localLimiterSize * numClients;

        // TODO a more robust solution is needed for live reloading -- probably we can take the delegate and rewrap it
        synchronized (globalLimiter) {
            if (globalLimiter.availablePermits() == -1) {
                globalLimiter.release(globalLimiterSize + 1);
            }
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
        if (localLimiter.tryAcquire()) {
            return applyAndRelease(localLimiter, function);
        } else if (globalLimiter.tryAcquire()) {
            return applyAndRelease(globalLimiter, function);
        }
        throw new TooManyRequestsException("RateLimitingLockService was unable to acquire a permit to assign a server "
                + "thread to the request.");
    }

    private <T, K extends Exception> T applyAndRelease(Semaphore semaphore,
            FunctionCheckedException<LockService, T, K> function) throws K {
        try {
            return function.apply(delegate);
        } finally {
            semaphore.release();
        }
    }

    @Value.Immutable
    public static abstract class RateLimitingConfiguration {
        public abstract boolean useRateLimiting();
        public abstract int availableThreads();
        public abstract int numClients();
    }

}
