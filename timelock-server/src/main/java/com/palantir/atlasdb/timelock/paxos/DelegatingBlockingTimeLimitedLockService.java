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
package com.palantir.atlasdb.timelock.paxos;

import java.math.BigInteger;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;
import javax.ws.rs.PathParam;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.SimpleTimeLimiter;
import com.google.common.util.concurrent.TimeLimiter;
import com.google.common.util.concurrent.UncheckedTimeoutException;
import com.palantir.lock.BlockingTimeoutException;
import com.palantir.lock.HeldLocksGrant;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockResponse;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.LockService;
import com.palantir.lock.SimpleHeldLocksToken;
import com.palantir.lock.impl.LockServiceImpl;

public class DelegatingBlockingTimeLimitedLockService implements LockService {
    private final LockService delegate;
    private final TimeLimiter timeLimiter;
    private final long timeLimitMillis;

    private DelegatingBlockingTimeLimitedLockService(
            LockService delegate,
            TimeLimiter timeLimiter,
            long timeLimitMillis) {
        this.delegate = delegate;
        this.timeLimiter = timeLimiter;
        this.timeLimitMillis = timeLimitMillis;
    }

    public LockService create(long timeLimitMillis) {
        return new DelegatingBlockingTimeLimitedLockService(
                LockServiceImpl.create(),
                new SimpleTimeLimiter(),
                timeLimitMillis);
    }

    @Nullable
    @Override
    public LockRefreshToken lock(@PathParam("client") String client, LockRequest request) throws InterruptedException {
        return callWithTimeoutUnchecked(() -> delegate.lock(client, request));
    }

    @Override
    public HeldLocksToken lockAndGetHeldLocks(@PathParam("client") String client, LockRequest request)
            throws InterruptedException {
        return callWithTimeoutUnchecked(() -> delegate.lockAndGetHeldLocks(client, request));
    }

    @Override
    public boolean unlock(LockRefreshToken token) {
        return delegate.unlock(token);
    }

    @Override
    public Set<LockRefreshToken> refreshLockRefreshTokens(Iterable<LockRefreshToken> tokens) {
        return null;
    }

    @Nullable
    @Override
    public Long getMinLockedInVersionId(@PathParam("client") String client) {
        return null;
    }

    @Override
    public LockResponse lockWithFullLockResponse(LockClient client, LockRequest request) throws InterruptedException {
        return null;
    }

    @Override
    public boolean unlock(HeldLocksToken token) {
        return false;
    }

    @Override
    public boolean unlockSimple(SimpleHeldLocksToken token) {
        return false;
    }

    @Override
    public boolean unlockAndFreeze(HeldLocksToken token) {
        return false;
    }

    @Override
    public Set<HeldLocksToken> getTokens(LockClient client) {
        return null;
    }

    @Override
    public Set<HeldLocksToken> refreshTokens(Iterable<HeldLocksToken> tokens) {
        return null;
    }

    @Nullable
    @Override
    public HeldLocksGrant refreshGrant(HeldLocksGrant grant) {
        return null;
    }

    @Nullable
    @Override
    public HeldLocksGrant refreshGrant(BigInteger grantId) {
        return null;
    }

    @Override
    public HeldLocksGrant convertToGrant(HeldLocksToken token) {
        return null;
    }

    @Override
    public HeldLocksToken useGrant(LockClient client, HeldLocksGrant grant) {
        return null;
    }

    @Override
    public HeldLocksToken useGrant(LockClient client, BigInteger grantId) {
        return null;
    }

    @Nullable
    @Override
    public Long getMinLockedInVersionId() {
        return null;
    }

    @Override
    public Long getMinLockedInVersionId(LockClient client) {
        return null;
    }

    @Override
    public LockServerOptions getLockServerOptions() {
        return null;
    }

    @Override
    public long currentTimeMillis() {
        return 0;
    }

    @Override
    public void logCurrentState() {

    }

    private <T> T callWithTimeoutUnchecked(Callable<T> callable) {
        try {
            return timeLimiter.callWithTimeout(callable, timeLimitMillis, TimeUnit.MILLISECONDS, true);
        } catch (UncheckedTimeoutException e) {
            throw new BlockingTimeoutException(e);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
