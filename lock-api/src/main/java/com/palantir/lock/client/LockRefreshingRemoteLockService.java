/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.lock.client;

import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.palantir.common.concurrent.PTExecutors;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.RemoteLockService;
import com.palantir.lock.SimpleTimeDuration;
import com.palantir.lock.TimeDuration;

public class LockRefreshingRemoteLockService implements RemoteLockService {

    private final RemoteLockService delegate;
    protected final LockRefresher refresher;

    public LockRefreshingRemoteLockService(RemoteLockService delegate) {
        this(delegate, PTExecutors.newScheduledThreadPoolExecutor(1,
                PTExecutors.newNamedThreadFactory(true)), SimpleTimeDuration.of(5, TimeUnit.SECONDS));
    }

    LockRefreshingRemoteLockService(RemoteLockService delegate,
            ScheduledExecutorService executor, TimeDuration refreshRate) {
        this.delegate = delegate;
        refresher = LockRefresher.create(delegate, executor, refreshRate);
    }

    @Override
    public LockRefreshToken lock(String client, LockRequest request) throws InterruptedException {
        LockRefreshToken ret = delegate.lock(client, request);
        if (ret != null) {
            refresher.startRefreshing(ret);
        }
        return ret;
    }

    @Override
    public HeldLocksToken lockAndGetHeldLocks(String client, LockRequest request) throws InterruptedException {
        HeldLocksToken ret = delegate.lockAndGetHeldLocks(client, request);
        if (ret != null) {
            refresher.startRefreshing(ret.getLockRefreshToken());
        }
        return ret;
    }

    @Override
    public boolean unlock(LockRefreshToken token) {
        refresher.stopRefreshing(token);
        return delegate.unlock(token);
    }

    public void dispose() {
        refresher.dispose();
    }

    @Override
    public Set<LockRefreshToken> refreshLockRefreshTokens(Iterable<LockRefreshToken> tokens) {
        return delegate.refreshLockRefreshTokens(tokens);
    }

    @Override
    public Long getMinLockedInVersionId(String client) {
        return delegate.getMinLockedInVersionId(client);
    }

    @Override
    public long currentTimeMillis() {
        return delegate.currentTimeMillis();
    }

    @Override
    public void logCurrentState() {
        delegate.logCurrentState();
    }
}
