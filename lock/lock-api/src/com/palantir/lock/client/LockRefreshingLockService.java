// Copyright 2015 Palantir Technologies
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.lock.client;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.lock.ForwardingLockService;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockResponse;
import com.palantir.lock.LockService;
import com.palantir.lock.SimpleHeldLocksToken;

public class LockRefreshingLockService extends ForwardingLockService {
    private static final Logger log = LoggerFactory.getLogger(LockRefreshingLockService.class);

    final LockService delegate;
    final Map<LockRefreshToken, HeldLocksToken> toRefresh;
    final ScheduledExecutorService exec;
    final long refreshFrequencyMillis = 5000;
    volatile boolean isClosed = false;

    public static LockRefreshingLockService create(LockService delegate) {
        final LockRefreshingLockService ret = new LockRefreshingLockService(delegate);
        ret.exec.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                long startTime = System.currentTimeMillis();
                try {
                    ret.refreshLocks();
                } catch (Throwable t) {
                    log.error("Failed to refresh locks", t);
                } finally {
                    long elapsed = System.currentTimeMillis() - startTime;

                    if (elapsed > LockRequest.DEFAULT_LOCK_TIMEOUT.toMillis()/2) {
                        log.error("Refreshing locks took " + elapsed + " milliseconds" +
                                " for tokens: " + ret.toRefresh);
                    } else if (elapsed > ret.refreshFrequencyMillis) {
                        log.warn("Refreshing locks took " + elapsed + " milliseconds" +
                                " for tokens: " + ret.toRefresh);
                    }
                }
            }
        }, 0, ret.refreshFrequencyMillis, TimeUnit.MILLISECONDS);
        return ret;
    }

    private LockRefreshingLockService(LockService delegate) {
        this.delegate = delegate;
        toRefresh = Maps.newConcurrentMap();
        exec = PTExecutors.newScheduledThreadPool(1, PTExecutors.newNamedThreadFactory(true));
    }

    @Override
    protected LockService delegate() {
        return delegate;
    }

    @Override
    public LockResponse lock(LockClient client, LockRequest request) throws InterruptedException {
        LockResponse lock = super.lock(client, request);
        if (lock.getToken() != null) {
            toRefresh.put(lock.getToken().getLockRefreshToken(), lock.getToken());
        }
        return lock;
    }

    private void refreshLocks() {
        ImmutableMap<LockRefreshToken, HeldLocksToken> refreshCopy = ImmutableMap.copyOf(toRefresh);
        Set<LockRefreshToken> refreshedTokens = delegate().refreshLockRefreshTokens(refreshCopy.keySet());
        for (LockRefreshToken token : refreshCopy.keySet()) {
            if (!refreshedTokens.contains(token)
                    && toRefresh.containsKey(token)) {
                log.error("failed to refresh lock: " + refreshCopy.get(token));
                toRefresh.remove(token);
            }
        }
    }

    @Override
    public boolean unlock(HeldLocksToken token) {
        toRefresh.remove(token.getLockRefreshToken());
        return super.unlock(token);
    }

    @Override
    public boolean unlockSimple(SimpleHeldLocksToken token) {
        toRefresh.remove(token.asLockRefreshToken());
        return super.unlockSimple(token);
    }

    @Override
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

}
