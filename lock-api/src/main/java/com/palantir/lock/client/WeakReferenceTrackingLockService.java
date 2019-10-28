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

package com.palantir.lock.client;

import java.lang.ref.WeakReference;
import java.math.BigInteger;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockService;
import com.palantir.lock.SimplifyingLockService;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeRuntimeException;

// Can we fold this into LockRefreshingLockService?
public final class WeakReferenceTrackingLockService extends SimplifyingLockService {

    private static final Logger log = LoggerFactory.getLogger(WeakReferenceTrackingLockService.class);

    private final LockService delegate;
    private final Map<BigInteger, TokenIdReference> liveTokens;

    public WeakReferenceTrackingLockService(LockService delegate, ScheduledExecutorService exec) {
        this.delegate = delegate;
        this.liveTokens = new ConcurrentHashMap<>();
        // TODO: Might want to only run this once every 30 seconds normally,
        // but schedule more aggressively if a leak is detected.
        exec.scheduleWithFixedDelay(() -> {
            try {
                logLeaks();
            } catch (Throwable t) {
                // Exit if InterruptedException
                log.warn("Failed when trying to detect lock leaks", t);
            }
        }, 0, 5, TimeUnit.SECONDS);
    }

    @Override
    public LockService delegate() {
        return delegate;
    }

    @Override
    public HeldLocksToken lockAndGetHeldLocks(String client, LockRequest request)
            throws InterruptedException {
        HeldLocksToken lock = super.lockAndGetHeldLocks(client, request);
        if (lock != null) {
            register(lock.getTokenId());
        }
        return lock;
    }

    @Override
    public boolean unlock(LockRefreshToken token) {
        unregister(token.getTokenId());
        return super.unlock(token);
    }

    private void register(BigInteger tokenId) {
        // Feels weird, but we're lacking a good instance to latch onto.
        BigInteger tokenIdClone = new BigInteger(tokenId.toByteArray());
        liveTokens.put(tokenIdClone, new TokenIdReference(tokenId, maybeCreateStackTrace()));
    }

    private void unregister(BigInteger tokenId) {
        liveTokens.remove(tokenId);
    }

    private void logLeaks() {
        liveTokens.values().removeIf(tokenIdReference -> {
            boolean shouldRemove = tokenIdReference.get() == null;
            if (shouldRemove) {
                logLeak(tokenIdReference.stacktrace);
            }
            return shouldRemove;
        });
    }

    private void logLeak(Optional<RuntimeException> stackTrace) {
        // TODO: Probably want to have some throttling in logging here;
        if (stackTrace.isPresent()) {
            log.warn("Lock leak detected - did you forget to unlock a lock?",
                    stackTrace.get());
        } else {
            log.warn("Lock leak detected - did you forget to unlock a lock?"
                            + "To get a "
                            + "stack trace for the call where the acquire happened, set log "
                            + "level to TRACE.",
                    SafeArg.of("loggerToSetToTrace", log.getName()));
        }
    }

    private static final class TokenIdReference extends WeakReference<BigInteger> {
        private final Optional<RuntimeException> stacktrace;

        TokenIdReference(BigInteger tokenId, Optional<RuntimeException> stacktrace) {
            super(tokenId);
            this.stacktrace = stacktrace;
        }
    }

    static Optional<RuntimeException> maybeCreateStackTrace() {
        if (log.isTraceEnabled()) {
            return Optional.of(new SafeRuntimeException("Runtime exception for stack trace"));
        }
        return Optional.empty();
    }
}
