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
package com.palantir.lock.client;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockResponse;
import com.palantir.lock.LockService;
import com.palantir.lock.SimpleHeldLocksToken;
import com.palantir.lock.SimplifyingLockService;
import com.palantir.logsafe.Preconditions;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class LockRefreshingLockService extends SimplifyingLockService {
    public static final int REFRESH_BATCH_SIZE = 500_000;
    private static final Logger log = LoggerFactory.getLogger(LockRefreshingLockService.class);

    final LockService delegate;
    final Set<LockRefreshToken> toRefresh;
    final ScheduledExecutorService exec;
    final long refreshFrequencyMillis = 5000;
    volatile boolean isClosed = false;

    public static LockRefreshingLockService create(LockService delegate) {
        final LockRefreshingLockService ret = new LockRefreshingLockService(delegate);
        ret.exec.scheduleWithFixedDelay(() -> {
            long startTime = System.currentTimeMillis();
            try {
                ret.refreshLocks();
            } catch (Throwable t) {
                log.warn("Failed to refresh locks", t);
            } finally {
                long elapsed = System.currentTimeMillis() - startTime;

                if (elapsed > LockRequest.getDefaultLockTimeout().toMillis() / 2) {
                    log.warn("Refreshing locks took {} milliseconds"
                            + " for tokens: {}", elapsed, ret.toRefresh);
                } else if (elapsed > ret.refreshFrequencyMillis) {
                    log.info("Refreshing locks took {} milliseconds"
                            + " for tokens: {}", elapsed, ret.toRefresh);
                }
            }
        }, 0, ret.refreshFrequencyMillis, TimeUnit.MILLISECONDS);
        return ret;
    }

    private LockRefreshingLockService(LockService delegate) {
        this.delegate = delegate;
        toRefresh = ConcurrentHashMap.newKeySet();
        exec = PTExecutors.newScheduledThreadPool(1, PTExecutors.newNamedThreadFactory(true));
    }

    @Override
    public LockService delegate() {
        Preconditions.checkState(!isClosed, "LockRefreshingLockService is closed");
        return delegate;
    }

    @Override
    public LockResponse lockWithFullLockResponse(LockClient client, LockRequest request) throws InterruptedException {
        LockResponse lock = super.lockWithFullLockResponse(client, request);
        if (lock.getToken() != null) {
            toRefresh.add(lock.getToken().getLockRefreshToken());
        }
        return lock;
    }

    @Override
    public HeldLocksToken lockAndGetHeldLocks(String client, LockRequest request)
            throws InterruptedException {
        HeldLocksToken lock = super.lockAndGetHeldLocks(client, request);
        if (lock != null) {
            toRefresh.add(lock.getLockRefreshToken());
        }
        return lock;
    }

    @Override
    public LockRefreshToken lock(String client, LockRequest request)
            throws InterruptedException {
        LockRefreshToken ret = super.lock(client, request);
        if (ret != null) {
            toRefresh.add(ret);
        }
        return ret;
    }

    @Override
    public boolean unlock(LockRefreshToken token) {
        toRefresh.remove(token);
        return super.unlock(token);
    }

    @Override
    public boolean unlock(HeldLocksToken token) {
        toRefresh.remove(token.getLockRefreshToken());
        return super.unlock(token);
    }

    private void refreshLocks() {
        ImmutableList<LockRefreshToken> refreshCopy = ImmutableList.copyOf(toRefresh);
        if (refreshCopy.isEmpty()) {
            return;
        }
        Set<LockRefreshToken> refreshedTokens = new HashSet<>();
        // We batch refreshes to avoid sending payload of excessive size
        for (List<LockRefreshToken> tokenBatch : Lists.partition(refreshCopy, REFRESH_BATCH_SIZE)) {
            refreshedTokens.addAll(delegate.refreshLockRefreshTokens(tokenBatch));
        }
        for (LockRefreshToken token : refreshCopy) {
            if (!refreshedTokens.contains(token)
                    && toRefresh.contains(token)) {
                log.warn("failed to refresh lock: {}", token);
                toRefresh.remove(token);
            }
        }
    }

    @Override
    public boolean unlockSimple(SimpleHeldLocksToken token) {
        toRefresh.remove(token.asLockRefreshToken());
        return super.unlockSimple(token);
    }

    @Override
    public boolean unlockAndFreeze(HeldLocksToken token) {
        toRefresh.remove(token.getLockRefreshToken());
        return super.unlockAndFreeze(token);
    }

    @Override
    @SuppressWarnings("checkstyle:NoFinalizer") // TODO (jkong): Can we safely remove this without breaking things?
    protected void finalize() throws Throwable {
        super.finalize();
        if (!isClosed) {
            log.warn("Closing in the finalize method.  This should be closed explicitly.");
            dispose();
        }
    }

    public void dispose() {
        exec.shutdown();
        isClosed = true;
    }

    @Override
    public void close() {
        if (!isClosed) {
            super.close();
            dispose();
        }
    }

    // visible for debugging clients at runtime
    public Set<LockRefreshToken> getAllRefreshingTokens() {
        return ImmutableSet.copyOf(toRefresh);
    }
}
