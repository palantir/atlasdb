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

import com.palantir.lock.ForwardingLockService;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockResponse;
import com.palantir.lock.LockService;

public class ThreadPooledLockService extends ForwardingLockService {
    private final ThreadPooledWrapper<LockService> wrapper;

    public ThreadPooledLockService(LockService delegate, int localThreadPoolSize, Semaphore sharedThreadPool) {
        wrapper = new ThreadPooledWrapper<>(delegate, localThreadPoolSize, sharedThreadPool);
    }

    @Override
    protected LockService delegate() {
        return wrapper.delegate();
    }

    @Nullable
    @Override
    public LockRefreshToken lock(String client, LockRequest request) throws InterruptedException {
        return wrapper.applyWithPermit(lockService -> lockService.lock(client, request));
    }

    @Override
    public HeldLocksToken lockAndGetHeldLocks(String client, LockRequest request) throws InterruptedException {
        return wrapper.applyWithPermit(lockService -> lockService.lockAndGetHeldLocks(client, request));
    }

    @Override
    public LockResponse lockWithFullLockResponse(LockClient client, LockRequest request) throws InterruptedException {
        return wrapper.applyWithPermit(lockService -> lockService.lockWithFullLockResponse(client, request));
    }

    @Override
    public Set<HeldLocksToken> refreshTokens(Iterable<HeldLocksToken> tokens) {
        return wrapper.applyWithPermit(lockService -> lockService.refreshTokens(tokens));
    }

    @Override
    public Set<LockRefreshToken> refreshLockRefreshTokens(Iterable<LockRefreshToken> tokens) {
        return wrapper.applyWithPermit(lockService -> lockService.refreshLockRefreshTokens(tokens));
    }
}
