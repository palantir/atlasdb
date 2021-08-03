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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.RateLimiter;
import com.palantir.common.base.Throwables;
import com.palantir.lock.v2.ClientLockingOptions;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionResponse;
import com.palantir.lock.v2.TimelockService;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.lock.v2.WaitForLocksResponse;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.timestamp.TimestampRange;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Logs operations on an underlying {@link TimelockService} that take a long time (defined as longer than
 * {@link ProfilingTimelockService#SLOW_THRESHOLD}). This logger is configured not to record more than one
 * operation every {@link ProfilingTimelockService#LOGGING_TIME_WINDOW}. It will log what the longest operation that
 * completed in the past {@link ProfilingTimelockService#LOGGING_TIME_WINDOW} window was, and how long it took. If no
 * operation took longer than {@link ProfilingTimelockService#SLOW_THRESHOLD} it will not log.
 *
 * The {@link ProfilingTimelockService} does not cover specific operations which are at risk for taking long
 * times owing to contention - in particular, this includes {@link #lock(LockRequest)} and
 * {@link #waitForLocks(WaitForLocksRequest)}.
 *
 * Profiling is done explicitly at this level (and not at {@link com.palantir.lock.v2.TimelockRpcClient} level)
 * to reflect the impact of timelock operations and cluster state changes (e.g. leader elections, rolling bounces) on
 * clients.
 */
public class ProfilingTimelockService implements AutoCloseable, TimelockService {
    private static final Logger log = LoggerFactory.getLogger(ProfilingTimelockService.class);

    @VisibleForTesting
    static final Duration SLOW_THRESHOLD = Duration.ofSeconds(1);

    private static final Duration LOGGING_TIME_WINDOW = Duration.ofSeconds(10);

    private final Logger logger;
    private final TimelockService delegate;
    private final Supplier<Stopwatch> stopwatchSupplier;

    private final AtomicReference<Optional<ActionProfile>> slowestOperation = new AtomicReference<>(Optional.empty());
    private final BooleanSupplier loggingPermissionSupplier;

    @VisibleForTesting
    ProfilingTimelockService(
            Logger logger,
            TimelockService delegate,
            Supplier<Stopwatch> stopwatchSupplier,
            BooleanSupplier loggingPermissionSupplier) {
        this.logger = logger;
        this.delegate = delegate;
        this.stopwatchSupplier = stopwatchSupplier;
        this.loggingPermissionSupplier = loggingPermissionSupplier;
    }

    public static ProfilingTimelockService create(TimelockService delegate) {
        RateLimiter rateLimiter = RateLimiter.create(1. / LOGGING_TIME_WINDOW.getSeconds());
        return new ProfilingTimelockService(log, delegate, Stopwatch::createStarted, rateLimiter::tryAcquire);
    }

    @Override
    public boolean isInitialized() {
        return delegate.isInitialized();
    }

    @Override
    public long getFreshTimestamp() {
        return runTaskTimed("getFreshTimestamp", delegate::getFreshTimestamp);
    }

    @Override
    public long getCommitTimestamp(long startTs, LockToken commitLocksToken) {
        return runTaskTimed("getCommitTimestamp", () -> delegate.getCommitTimestamp(startTs, commitLocksToken));
    }

    @Override
    public TimestampRange getFreshTimestamps(int numTimestampsRequested) {
        return runTaskTimed("getFreshTimestamps", () -> delegate.getFreshTimestamps(numTimestampsRequested));
    }

    @Override
    public LockImmutableTimestampResponse lockImmutableTimestamp() {
        // N.B. Immutable timestamp lock is not exclusive, so it should be fast.
        return runTaskTimed("lockImmutableTimestamp", delegate::lockImmutableTimestamp);
    }

    @Override
    public LockImmutableTimestampResponse lockSpecificImmutableTimestamp(long userTimestamp) {
        return runTaskTimed(
                "lockSpecificImmutableTimestamp", () -> delegate.lockSpecificImmutableTimestamp(userTimestamp));
    }

    @Override
    public List<StartIdentifiedAtlasDbTransactionResponse> startIdentifiedAtlasDbTransactionBatch(int count) {
        return runTaskTimed(
                "startIdentifiedAtlasDbTransactionBatch", () -> delegate.startIdentifiedAtlasDbTransactionBatch(count));
    }

    @Override
    public long getImmutableTimestamp() {
        return runTaskTimed("getImmutableTimestamp", delegate::getImmutableTimestamp);
    }

    @Override
    public LockResponse lock(LockRequest request) {
        // Don't profile this, as it may be skewed by user contention on locks.
        tryFlushLogs();
        return delegate.lock(request);
    }

    @Override
    public LockResponse lock(LockRequest lockRequest, ClientLockingOptions options) {
        // Don't profile this, as it may be skewed by user contention on locks.
        tryFlushLogs();
        return delegate.lock(lockRequest, options);
    }

    @Override
    public WaitForLocksResponse waitForLocks(WaitForLocksRequest request) {
        // Don't profile this, as it may be skewed by user contention on locks.
        tryFlushLogs();
        return delegate.waitForLocks(request);
    }

    @Override
    public Set<LockToken> refreshLockLeases(Set<LockToken> tokens) {
        return runTaskTimed("refreshLockLeases", () -> delegate.refreshLockLeases(tokens));
    }

    @Override
    public Set<LockToken> unlock(Set<LockToken> tokens) {
        return runTaskTimed("unlock", () -> delegate.unlock(tokens));
    }

    @Override
    public void tryUnlock(Set<LockToken> tokens) {
        delegate.tryUnlock(tokens);
    }

    @Override
    public long currentTimeMillis() {
        return runTaskTimed("currentTimeMillis", delegate::currentTimeMillis);
    }

    private <T> T runTaskTimed(String actionName, Supplier<T> action) {
        Stopwatch stopwatch = stopwatchSupplier.get();
        try {
            T result = action.get();
            trackSuccessfulAction(actionName, stopwatch);
            return result;
        } catch (RuntimeException e) {
            trackFailedAction(actionName, stopwatch, e);
            throw e;
        }
    }

    private void trackSuccessfulAction(String actionName, Stopwatch stopwatch) {
        trackActionAndMaybeLog(actionName, stopwatch, Optional.empty());
    }

    private void trackFailedAction(String actionName, Stopwatch stopwatch, RuntimeException exception) {
        trackActionAndMaybeLog(actionName, stopwatch, Optional.of(exception));
    }

    private void trackActionAndMaybeLog(String actionName, Stopwatch stopwatch, Optional<Exception> failure) {
        stopwatch.stop();
        accumulateSlowOperationTracking(actionName, stopwatch, failure);
        tryFlushLogs();
    }

    private void accumulateSlowOperationTracking(String actionName, Stopwatch stopwatch, Optional<Exception> failure) {
        if (stopwatchDescribesSlowOperation(stopwatch)) {
            slowestOperation.accumulateAndGet(
                    Optional.of(ImmutableActionProfile.of(actionName, stopwatch.elapsed(), failure)),
                    (existing, update) -> {
                        if (!existing.isPresent()) {
                            return update;
                        }
                        return existing.get().duration().compareTo(stopwatch.elapsed()) < 0 ? update : existing;
                    });
        }
    }

    private void tryFlushLogs() {
        if (loggingPermissionSupplier.getAsBoolean()) {
            Optional<ActionProfile> actionProfile = slowestOperation.getAndSet(Optional.empty());
            if (actionProfile.isPresent()) {
                ActionProfile presentActionProfile = actionProfile.get();
                logger.info(
                        "Call to TimeLockService#{} took {}. This was the slowest operation that completed"
                                + " in the last {}. The operation completed with outcome {} - if it failed, the error"
                                + " was {}.",
                        SafeArg.of("actionName", presentActionProfile.actionName()),
                        SafeArg.of("duration", presentActionProfile.duration()),
                        SafeArg.of("timeWindow", LOGGING_TIME_WINDOW),
                        SafeArg.of("outcome", presentActionProfile.failure().isPresent() ? "fail" : "success"),
                        UnsafeArg.of("failure", presentActionProfile.failure()));
            }
        }
    }

    private static boolean stopwatchDescribesSlowOperation(Stopwatch stopwatch) {
        return stopwatch.elapsed().compareTo(SLOW_THRESHOLD) >= 0;
    }

    @Override
    public void close() {
        if (delegate instanceof AutoCloseable) {
            try {
                ((AutoCloseable) delegate).close();
            } catch (Exception e) {
                throw Throwables.rewrapAndThrowUncheckedException("Error closing delegate timelock service", e);
            }
        }
    }

    @Value.Immutable
    interface ActionProfile {
        @Value.Parameter
        String actionName();

        @Value.Parameter
        Duration duration();

        @Value.Parameter
        Optional<Exception> failure();
    }
}
