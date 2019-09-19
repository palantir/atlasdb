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
package com.palantir.atlasdb.timelock.lock;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.SimpleTimeLimiter;
import com.google.common.util.concurrent.TimeLimiter;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.lock.CloseableLockService;
import com.palantir.lock.HeldLocksGrant;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockResponse;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.SimpleHeldLocksToken;
import com.palantir.lock.remoting.BlockingTimeoutException;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;
import javax.ws.rs.PathParam;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlockingTimeLimitedLockService implements CloseableLockService {
    private static final Logger log = LoggerFactory.getLogger(BlockingTimeLimitedLockService.class);

    private final CloseableLockService delegate;
    private final TimeLimiter timeLimiter;
    private final long blockingTimeLimitMillis;

    @VisibleForTesting
    BlockingTimeLimitedLockService(
            CloseableLockService delegate,
            TimeLimiter timeLimiter,
            long blockingTimeLimitMillis) {
        this.delegate = delegate;
        this.timeLimiter = timeLimiter;
        this.blockingTimeLimitMillis = blockingTimeLimitMillis;
    }

    public static BlockingTimeLimitedLockService create(CloseableLockService lockService,
            long blockingTimeLimitMillis) {
        // TODO (jkong): Inject the executor to allow application lifecycle managed executors.
        // Currently maintaining existing behaviour.
        return new BlockingTimeLimitedLockService(
                lockService,
                SimpleTimeLimiter.create(PTExecutors.newCachedThreadPool()),
                blockingTimeLimitMillis);
    }

    @Nullable
    @Override
    public LockRefreshToken lock(@PathParam("client") String client, LockRequest request) throws InterruptedException {
        return callWithTimeLimit(
                () -> delegate.lock(client, request),
                ImmutableLockRequestSpecification.of("lock", client, request));
    }

    @Override
    public HeldLocksToken lockAndGetHeldLocks(@PathParam("client") String client, LockRequest request)
            throws InterruptedException {
        return callWithTimeLimit(
                () -> delegate.lockAndGetHeldLocks(client, request),
                ImmutableLockRequestSpecification.of("lockAndGetHeldLocks", client, request));
    }

    @Override
    public Set<LockRefreshToken> refreshLockRefreshTokens(Iterable<LockRefreshToken> tokens) {
        return delegate.refreshLockRefreshTokens(tokens);
    }

    @Override
    public LockResponse lockWithFullLockResponse(LockClient client, LockRequest request) throws InterruptedException {
        return callWithTimeLimit(
                () -> delegate.lockWithFullLockResponse(client, request),
                ImmutableLockRequestSpecification.of("lockWithFullLockResponse", client.getClientId(), request));
    }

    @Override
    public boolean unlock(HeldLocksToken token) {
        return delegate.unlock(token);
    }

    @Override
    public boolean unlock(LockRefreshToken token) {
        return delegate.unlock(token);
    }

    @Override
    public boolean unlockSimple(SimpleHeldLocksToken token) {
        return delegate.unlockSimple(token);
    }

    @Override
    public boolean unlockAndFreeze(HeldLocksToken token) {
        return delegate.unlockAndFreeze(token);
    }

    @Override
    public Set<HeldLocksToken> getTokens(LockClient client) {
        return delegate.getTokens(client);
    }

    @Override
    public Set<HeldLocksToken> refreshTokens(Iterable<HeldLocksToken> tokens) {
        return delegate.refreshTokens(tokens);
    }

    @Nullable
    @Override
    public HeldLocksGrant refreshGrant(HeldLocksGrant grant) {
        return delegate.refreshGrant(grant);
    }

    @Nullable
    @Override
    public HeldLocksGrant refreshGrant(BigInteger grantId) {
        return delegate.refreshGrant(grantId);
    }

    @Override
    public HeldLocksGrant convertToGrant(HeldLocksToken token) {
        return delegate.convertToGrant(token);
    }

    @Override
    public HeldLocksToken useGrant(LockClient client, HeldLocksGrant grant) {
        return delegate.useGrant(client, grant);
    }

    @Override
    public HeldLocksToken useGrant(LockClient client, BigInteger grantId) {
        return delegate.useGrant(client, grantId);
    }

    @Nullable
    @Override
    public Long getMinLockedInVersionId(@PathParam("client") String client) {
        return delegate.getMinLockedInVersionId(client);
    }

    @Nullable
    @Override
    public Long getMinLockedInVersionId() {
        return delegate.getMinLockedInVersionId();
    }

    @Override
    public Long getMinLockedInVersionId(LockClient client) {
        return delegate.getMinLockedInVersionId(client);
    }

    @Override
    public LockServerOptions getLockServerOptions() {
        return delegate.getLockServerOptions();
    }

    @Override
    public long currentTimeMillis() {
        return delegate.currentTimeMillis();
    }

    @Override
    public void logCurrentState() {
        delegate.logCurrentState();
    }

    private <T> T callWithTimeLimit(Callable<T> callable, LockRequestSpecification specification)
            throws InterruptedException {
        try {
            return timeLimiter.callWithTimeout(callable, blockingTimeLimitMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            // In this case, the thread was interrupted for some other reason, perhaps because we lost leadership.
            logInterrupted(specification, e);
            throw e;
        } catch (TimeoutException e) {
            // This is the legitimate timeout case we're trying to catch.
            throw logAndHandleTimeout(specification);
        } catch (Exception e) {
            // We don't know, and would prefer not to throw checked exceptions apart from InterruptedException.
            Throwable cause = e.getCause();
            if (cause instanceof InterruptedException) {
                logInterrupted(specification, (InterruptedException) cause);
                throw (InterruptedException) cause;
            }
            throw Throwables.propagate(e);
        }
    }

    private static void logInterrupted(LockRequestSpecification specification, InterruptedException e) {
        log.info("Lock service was interrupted when servicing {} for client \"{}\"; request was {}",
                SafeArg.of("method", specification.method()),
                SafeArg.of("client", specification.client()),
                UnsafeArg.of("lockRequest", specification.lockRequest()),
                e);
    }

    private BlockingTimeoutException logAndHandleTimeout(LockRequestSpecification specification) {
        final String logMessage = "Lock service timed out after {} milliseconds"
                + " when servicing {} for client \"{}\"";
        log.info(logMessage,
                SafeArg.of("timeoutDurationMillis", blockingTimeLimitMillis),
                SafeArg.of("method", specification.method()),
                SafeArg.of("client", specification.client()),
                UnsafeArg.of("lockRequest", specification.lockRequest()));

        String errorMessage = String.format(
                logMessage.replace("{}", "%s"),
                blockingTimeLimitMillis,
                specification.method(),
                specification.client());
        return new BlockingTimeoutException(errorMessage);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Value.Immutable
    abstract static class LockRequestSpecification {
        @Value.Parameter
        abstract String method();

        @Value.Parameter
        abstract String client();

        @Value.Parameter
        abstract LockRequest lockRequest();
    }
}
