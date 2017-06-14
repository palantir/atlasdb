/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.timelock.lock;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;
import javax.ws.rs.PathParam;

import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.SimpleTimeLimiter;
import com.google.common.util.concurrent.TimeLimiter;
import com.google.common.util.concurrent.UncheckedTimeoutException;
import com.palantir.lock.CloseableRemoteLockService;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.remoting.BlockingTimeoutException;

public class BlockingTimeLimitedLockService implements CloseableRemoteLockService {
    private static final Logger log = LoggerFactory.getLogger(BlockingTimeLimitedLockService.class);

    private final CloseableRemoteLockService delegate;
    private final TimeLimiter timeLimiter;
    private final long blockingTimeLimitMillis;

    @VisibleForTesting
    BlockingTimeLimitedLockService(
            CloseableRemoteLockService delegate,
            TimeLimiter timeLimiter,
            long blockingTimeLimitMillis) {
        this.delegate = delegate;
        this.timeLimiter = timeLimiter;
        this.blockingTimeLimitMillis = blockingTimeLimitMillis;
    }

    public static BlockingTimeLimitedLockService create(CloseableRemoteLockService lockService,
            long blockingTimeLimitMillis) {
        return new BlockingTimeLimitedLockService(lockService, new SimpleTimeLimiter(), blockingTimeLimitMillis);
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
    public boolean unlock(LockRefreshToken token) {
        return delegate.unlock(token);
    }

    @Override
    public Set<LockRefreshToken> refreshLockRefreshTokens(Iterable<LockRefreshToken> tokens) {
        return delegate.refreshLockRefreshTokens(tokens);
    }

    @Nullable
    @Override
    public Long getMinLockedInVersionId(@PathParam("client") String client) {
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

    @SuppressWarnings("Slf4jConstantLogMessage") // TODO (jkong): Implement an exception producer-and-logger
    private <T> T callWithTimeLimit(Callable<T> callable, LockRequestSpecification specification)
            throws InterruptedException {
        try {
            return timeLimiter.callWithTimeout(callable, blockingTimeLimitMillis, TimeUnit.MILLISECONDS, true);
        } catch (InterruptedException e) {
            // In this case, the thread was interrupted for some other reason, perhaps because we lost leadership.
            log.info("Lock service was interrupted when servicing {} for client \"{}\"; request was {}",
                    specification.method(),
                    specification.client(),
                    specification.lockRequest(),
                    e);
            throw e;
        } catch (UncheckedTimeoutException e) {
            // This is the legitimate timeout case we're trying to catch.
            String message = String.format(
                    "Lock service timed out after %s milliseconds when servicing %s for client \"%s\"; request was %s",
                    blockingTimeLimitMillis,
                    specification.method(),
                    specification.client(),
                    specification.lockRequest());
            log.info(message, e);
            throw new BlockingTimeoutException(message);
        } catch (Exception e) {
            // We don't know, and would prefer not to throw checked exceptions apart from InterruptedException.
            throw Throwables.propagate(e);
        }
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
