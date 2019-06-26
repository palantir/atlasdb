/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.lock;

import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import javax.ws.rs.NotSupportedException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.RateLimiter;
import com.palantir.lock.LockDescriptor;
import com.palantir.logsafe.SafeArg;

public class TargetedSweepAsyncLock implements AsyncLock {

    private static final Logger log = LoggerFactory.getLogger(TargetedSweepAsyncLock.class);
    private final AsyncLock delegateLock;
    private final RateLimiter rateLimiter;
    private final BooleanSupplier isRateLimited;
    private final ScheduledExecutorService timeoutExecutor;
    private final TargetedSweepLockUnlocker unlocker;

    TargetedSweepAsyncLock(
            AsyncLock delegate,
            RateLimiter rateLimiter,
            BooleanSupplier isRateLimited,
            ScheduledExecutorService timeoutExecutor,
            TargetedSweepLockUnlocker unlocker) {
        this.delegateLock = delegate;
        this.rateLimiter = rateLimiter;
        this.isRateLimited = isRateLimited;
        this.timeoutExecutor = timeoutExecutor;
        this.unlocker = unlocker;
    }

    /*
        lock, unlock, timeout are synchronised, so we can't get into a situation where we've acquired a lock, but are
        waiting for to acquire the rate limit permit, and then someone either times out the request id, unlocks, or does
        some weird stuff
     */
    @Override
    public synchronized AsyncResult<Void> lock(UUID requestId) {
        return delegateLock.lock(requestId)
                .concatWith(() -> {
                    /*
                    This is run either by
                        - lock if there are no existing locks
                        - unlock by the previous request id

                    Make both of these methods synchronised such that we can't sneak in a lock/unlock request to the
                    underlying lock and have two handlers running concurrently e.g. long gc pause

                    Downside of this approach means that if you get multiple lock requests whilst a lock is being
                    acquired and whilst waiting to acquire a rate limiter permit, the lock requests will wait for the
                    permit to be acquired.
                     */
                    if (isRateLimited.getAsBoolean()) {
                        rateLimiter.acquire();
                    }
                    return AsyncResult.completedResult();
                });
    }

    @Override
    public AsyncResult<Void> waitUntilAvailable(UUID requestId) {
        log.error("received waitUntilAvailable on a lock believed to be a targeted sweep lock",
                SafeArg.of("requestId", requestId));
        // we shouldn't hit this in practice
        throw new NotSupportedException();
    }

    /*
        This is called when:
            - the lock has expired
            - timed out (we can control this a bit more)
            - on error
            - or explicitly called

        This will also complete the next lock request if any, which can block depending on the rate limit.

        Since locks are asynchronously unlocked and the unlocks are batched together, you end up getting head of line
        blocking for any non-targeted sweep locks batched together with a targeted sweep locks to be unlocked.

        Given a batch of LockTokens you would have to hope that the targeted sweep locks are processed last, but that's
        not a given, and we can't control it from the client otherwise we'd have fixed the bug in the first place.

        If we get an unlock request for a targeted sweep lock, we should then process this asynchronously so that this
        method returns quickly. We don't care so much about holding to the targeted sweep lock for a fraction longer,
        as we are trying to rate limit it anyway!
     */
    @Override
    @SuppressWarnings("FutureReturnValueIgnored")
    public synchronized void unlock(UUID requestId) {
        if (isRateLimited.getAsBoolean()) {
            unlocker.unlock(delegateLock, requestId);
        } else {
            delegateLock.unlock(requestId);
        }
    }

    /*
        Targeted sweep requests a lock with acquire timeout of 100ms.

        So whilst it's all well and good to rate limit the lock, we want to hold onto any requests that successfully
        acquire the lock and add an artificial delay. In doing so, it means they can't request another lock 50ms later,
        thus reducing the rate of requests on timelock.

        Targeted sweep has an acquire timeout of 100ms, so if we've acquired the lock, but not the rate limit permit, we
        don't want to timeout the request as that would mean 50ms later we try again and we'll never get the lock. So we
        just extend any timeout by a second.

        If a lock is legitimately held, and *another* request wants to acquire it, then the new request will wait up to
        a second. This is fine, because it means that the request doesn't time out after 100ms and try again in 50ms, so
        this also reduces the rate of requests on timelock.

        This is synchronized => you can only timeout a request if the lock is either held or not held, not whilst
        acquiring the lock for the next request in the queue.
     */
    @Override
    public synchronized void timeout(UUID requestId) {
        if (isRateLimited.getAsBoolean()) {
            timeoutExecutor.schedule(() -> delegateLock.timeout(requestId), 1, TimeUnit.SECONDS);
        } else {
            delegateLock.timeout(requestId);
        }
    }

    @Override
    public LockDescriptor getDescriptor() {
        return delegateLock.getDescriptor();
    }
}
