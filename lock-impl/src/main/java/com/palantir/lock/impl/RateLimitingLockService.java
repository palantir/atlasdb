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

import java.util.Set;

import javax.annotation.Nullable;

import com.palantir.lock.ForwardingLockService;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockService;

public class RateLimitingLockService extends ForwardingLockService {
    private static final ThreadCountLimiter globalLimiter = new ThreadCountLimiter(-1);

    final LockService delegate;
    final ThreadCountLimiter limiter;

    private RateLimitingLockService(LockService delegate, int localLimiterSize) {
        this.delegate = delegate;
        this.limiter = new ThreadCountLimiter(localLimiterSize);
    }

    public static RateLimitingLockService create(LockService delegate, int availableThreads, int numClients) {
        // TODO availableThreads is non-negative due to dropwizard asserts, but still consider making this look better
        numClients = Math.max(numClients, 1);

        int localLimiterSize = availableThreads / numClients / 2;
        int globalLimiterSize = availableThreads - localLimiterSize * numClients;

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
        if (limiter.tryAcquire()) {
            try{
                Thread.sleep(100);
                LockRefreshToken token = delegate.lock(client, request);
                return token;
            } finally {
                limiter.release();
            }
        } else if (globalLimiter.tryAcquire()) {
            try{
                Thread.sleep(100);
                LockRefreshToken token = delegate.lock(client, request);
                return token;
            } finally {
                globalLimiter.release();
            }
        } else {
            throw new TooManyRequestsException();
        }
    }


    @Override
    public boolean unlock(LockRefreshToken token) {
        return delegate.unlock(token);
    }

    @Override
    public Set<LockRefreshToken> refreshLockRefreshTokens(Iterable<LockRefreshToken> tokens) {
        if (limiter.tryAcquire()) {
            Set<LockRefreshToken> result = delegate.refreshLockRefreshTokens(tokens);
            limiter.release();
            return result;
        } else if (globalLimiter.tryAcquire()) {
            Set<LockRefreshToken> result = delegate.refreshLockRefreshTokens(tokens);
            globalLimiter.release();
            return result;
        } else {
            throw new TooManyRequestsException();
        }
    }

    public static class TestSlowRateLimitingService extends RateLimitingLockService {

        private TestSlowRateLimitingService(LockService delegate, int localLimiterSize) {
            super(delegate, localLimiterSize);
        }


        public static RateLimitingLockService create(LockService delegate, int availableThreads, int numClients) {
            numClients = Math.max(numClients, 1);

            int localLimiterSize = availableThreads / numClients / 2;
            int globalLimiterSize = availableThreads - localLimiterSize * numClients;

            if (globalLimiter.availablePermits() == -1){
                globalLimiter.release(globalLimiterSize + 1);
            }
            return new TestSlowRateLimitingService(delegate, localLimiterSize);
        }
        @Nullable
        @Override
        public LockRefreshToken lock(String client, LockRequest request) throws InterruptedException {
            if (limiter.tryAcquire()) {
                try{
                    Thread.sleep(100);
                    LockRefreshToken token = delegate.lock(client, request);
                    return token;
                } finally {
                    limiter.release();
                }
            } else if (globalLimiter.tryAcquire()) {
                try{
                    Thread.sleep(100);
                    LockRefreshToken token = delegate.lock(client, request);
                    return token;
                } finally {
                    globalLimiter.release();
                }
            } else {
                throw new TooManyRequestsException();
            }
        }
    }
}
