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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.SimpleTimeLimiter;
import com.google.common.util.concurrent.TimeLimiter;
import com.google.common.util.concurrent.UncheckedTimeoutException;
import com.palantir.lock.HeldLocksGrant;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockResponse;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.LockService;
import com.palantir.lock.SimpleHeldLocksToken;
import com.palantir.lock.remoting.BlockingTimeoutException;

public class BlockingTimeLimitedLockService implements LockService {
    private static final Logger log = LoggerFactory.getLogger(BlockingTimeLimitedLockService.class);

    private final LockService delegate;
    private final TimeLimiter timeLimiter;
    private final long blockingTimeLimitMillis;

    private BlockingTimeLimitedLockService(
            LockService delegate,
            TimeLimiter timeLimiter,
            long blockingTimeLimitMillis) {
        this.delegate = delegate;
        this.timeLimiter = timeLimiter;
        this.blockingTimeLimitMillis = blockingTimeLimitMillis;
    }

    public static LockService create(LockService lockService, long blockingTimeLimitMillis) {
        return new BlockingTimeLimitedLockService(lockService, new SimpleTimeLimiter(), blockingTimeLimitMillis);
    }

    @Nullable
    @Override
    public LockRefreshToken lock(@PathParam("client") String client, LockRequest request) throws InterruptedException {
        return callWithTimeLimit(() -> delegate.lock(client, request), "lock", client, request);
    }

    @Override
    public HeldLocksToken lockAndGetHeldLocks(@PathParam("client") String client, LockRequest request)
            throws InterruptedException {
        return null;
    }

    @Override
    public boolean unlock(LockRefreshToken token) {
        return false;
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

    private <T> T callWithTimeLimit(Callable<T> callable, String method, String client, LockRequest lockRequest)
            throws InterruptedException {
        try {
            return timeLimiter.callWithTimeout(callable, blockingTimeLimitMillis, TimeUnit.MILLISECONDS, true);
        } catch (InterruptedException e) {
            // In this case, the thread was interrupted for some other reason, perhaps because we lost leadership.
            // This is fine but it's probably worth logging.
            log.warn("Lock service was interrupted when servicing {} for client {}; request was {}",
                    method, client, lockRequest, e);
            throw e;
        } catch (UncheckedTimeoutException e) {
            // This is the legitimate timeout case we're trying to catch.
            String errorMessage = String.format(
                    "Lock service timed out after %s milliseconds when servicing %s for client %s; request was %s",
                    blockingTimeLimitMillis,
                    method,
                    client,
                    lockRequest);
            log.warn(errorMessage, e);
            throw new BlockingTimeoutException(errorMessage);
        } catch (Exception e) {
            // We don't know, and would prefer not to throw checked exceptions apart from InterruptedException.
            throw Throwables.propagate(e);
        }
    }
}
