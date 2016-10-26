/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
 */

package com.palantir.lock.client;

import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.RemoteLockService;
import com.palantir.lock.TimeDuration;

class LockRefresher {
    private static final Logger log = LoggerFactory.getLogger(LockRefresher.class);

    private final RemoteLockService delegate;
    private final Set<LockRefreshToken> toRefresh;
    private final ScheduledExecutorService exec;
    private final long refreshFrequencyMillis;
    private volatile boolean isClosed = false;

    public static LockRefresher create(RemoteLockService delegate,
            ScheduledExecutorService executor, TimeDuration refreshRate) {
        LockRefresher refresher = new LockRefresher(delegate, executor, refreshRate);
        refresher.scheduleRefresh();
        return refresher;
    }

    private LockRefresher(RemoteLockService delegate,
            ScheduledExecutorService executor, TimeDuration refreshRate) {
        this.delegate = delegate;
        toRefresh = Sets.newConcurrentHashSet();
        this.exec = executor;
        refreshFrequencyMillis = refreshRate.toMillis();
    }

    void scheduleRefresh() {
        exec.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                long startTime = System.currentTimeMillis();
                try {
                    refreshLocks();
                } catch (Throwable t) {
                    log.error("Failed to refresh locks", t);
                } finally {
                    long elapsed = System.currentTimeMillis() - startTime;

                    if (elapsed > LockRequest.DEFAULT_LOCK_TIMEOUT.toMillis()/2) {
                        log.error("Refreshing locks took " + elapsed + " milliseconds" +
                                " for tokens: " + toRefresh);
                    } else if (elapsed > refreshFrequencyMillis) {
                        log.warn("Refreshing locks took " + elapsed + " milliseconds" +
                                " for tokens: " + toRefresh);
                    }
                }
            }
        }, 0, refreshFrequencyMillis, TimeUnit.MILLISECONDS);
    }

    private void refreshLocks() {
        ImmutableSet<LockRefreshToken> refreshCopy = ImmutableSet.copyOf(toRefresh);
        if (refreshCopy.isEmpty()) {
            return;
        }
        Set<LockRefreshToken> refreshedTokens = delegate.refreshLockRefreshTokens(refreshCopy);
        for (LockRefreshToken token : refreshCopy) {
            if (!refreshedTokens.contains(token)
                    && toRefresh.contains(token)) {
                log.error("failed to refresh lock: " + token);
                toRefresh.remove(token);
            }
        }
    }

    public boolean startRefreshing(LockRefreshToken token) {
        return toRefresh.add(token);
    }

    public boolean stopRefreshing(LockRefreshToken token) {
        return toRefresh.remove(token);
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
