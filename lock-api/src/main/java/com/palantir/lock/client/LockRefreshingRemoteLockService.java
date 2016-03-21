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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.lock.ForwardingRemoteLockService;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.RemoteLockService;

public class LockRefreshingRemoteLockService extends ForwardingRemoteLockService {
    private static final Logger log = LoggerFactory.getLogger(LockRefreshingRemoteLockService.class);

    final RemoteLockService delegate;
    final Set<LockRefreshToken> toRefresh;
    final ScheduledExecutorService exec;
    final long refreshFrequencyMillis = 5000;
    volatile boolean isClosed = false;

    public static LockRefreshingRemoteLockService create(RemoteLockService delegate) {
        final LockRefreshingRemoteLockService ret = new LockRefreshingRemoteLockService(delegate);
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

    private LockRefreshingRemoteLockService(RemoteLockService delegate) {
        this.delegate = delegate;
        toRefresh = Sets.newConcurrentHashSet();
        exec = PTExecutors.newScheduledThreadPool(1, PTExecutors.newNamedThreadFactory(true));
    }

    @Override
    protected RemoteLockService delegate() {
        return delegate;
    }

    @Override
    public LockRefreshToken lock(String client, LockRequest request) throws InterruptedException {
        LockRefreshToken ret = super.lock(client, request);
        if (ret != null) {
            toRefresh.add(ret);
        }
        return ret;
    }

    @Override
    public HeldLocksToken lockAndGetHeldLocks(String client, LockRequest request) throws InterruptedException {
        HeldLocksToken ret = super.lockAndGetHeldLocks(client, request);
        if (ret != null) {
            toRefresh.add(ret.getLockRefreshToken());
        }
        return ret;
    }

    @Override
    public boolean unlock(LockRefreshToken token) {
        toRefresh.remove(token);
        return super.unlock(token);
    }

    private void refreshLocks() {
        ImmutableSet<LockRefreshToken> refreshCopy = ImmutableSet.copyOf(toRefresh);
        if (refreshCopy.isEmpty()) {
            return;
        }
        Set<LockRefreshToken> refreshedTokens = delegate().refreshLockRefreshTokens(refreshCopy);
        for (LockRefreshToken token : refreshCopy) {
            if (!refreshedTokens.contains(token)
                    && toRefresh.contains(token)) {
                log.error("failed to refresh lock: " + token);
                toRefresh.remove(token);
            }
        }
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
