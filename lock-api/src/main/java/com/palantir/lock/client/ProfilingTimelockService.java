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

import java.io.Closeable;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.RateLimiter;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionResponse;
import com.palantir.lock.v2.TimelockService;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.lock.v2.WaitForLocksResponse;
import com.palantir.logsafe.SafeArg;
import com.palantir.timestamp.TimestampRange;

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

    private static final Duration SLOW_THRESHOLD = Duration.ofSeconds(1);
    private static final Duration LOGGING_TIME_WINDOW = Duration.ofSeconds(10);

    private final TimelockService delegate;
    private final Supplier<Stopwatch> stopwatchSupplier;

    private final AtomicReference<Optional<ActionNameAndDuration>> slowestOperation
            = new AtomicReference<>(Optional.empty());
    private final RateLimiter rateLimiter;

    private ProfilingTimelockService(
            TimelockService delegate,
            Supplier<Stopwatch> stopwatchSupplier,
            RateLimiter rateLimiter) {
        this.delegate = delegate;
        this.stopwatchSupplier = stopwatchSupplier;
        this.rateLimiter = rateLimiter;
    }

    public static ProfilingTimelockService create(TimelockService delegate) {
        return new ProfilingTimelockService(
                delegate, Stopwatch::createStarted, RateLimiter.create(1. / LOGGING_TIME_WINDOW.getSeconds()));
    }

    @Override
    public long getFreshTimestamp() {
        return runTaskTimed("getFreshTimestamp", delegate::getFreshTimestamp);
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
    public StartIdentifiedAtlasDbTransactionResponse startIdentifiedAtlasDbTransaction() {
        return runTaskTimed("startIdentifiedAtlasDbTransaction", delegate::startIdentifiedAtlasDbTransaction);
    }

    @Override
    public long getImmutableTimestamp() {
        return runTaskTimed("getImmutableTimestamp", delegate::getImmutableTimestamp);
    }

    @Override
    public LockResponse lock(LockRequest request) {
        // Don't profile this, as it may be skewed by user contention on locks.
        return delegate.lock(request);
    }

    @Override
    public WaitForLocksResponse waitForLocks(WaitForLocksRequest request) {
        // Don't profile this, as it may be skewed by user contention on locks.
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
    public long currentTimeMillis() {
        return runTaskTimed("currentTimeMillis", delegate::currentTimeMillis);
    }

    private <T> T runTaskTimed(String actionName, Supplier<T> action) {
        Stopwatch stopwatch = stopwatchSupplier.get();
        T result = action.get();
        submitActionTimeAndMaybeLog(actionName, stopwatch);
        return result;
    }

    private void submitActionTimeAndMaybeLog(String actionName, Stopwatch stopwatch) {
        stopwatch.stop();
        if (stopwatchDescribesSlowOperation(stopwatch)) {
            slowestOperation.accumulateAndGet(
                    Optional.of(ImmutableActionNameAndDuration.of(actionName, stopwatch.elapsed())),
                    (current, update) -> !update.isPresent()
                            || update.get().duration().compareTo(stopwatch.elapsed()) < 0
                            ? current
                            : update);
        }

        if (rateLimiter.tryAcquire()) {
            Optional<ActionNameAndDuration> actionNameAndDuration = slowestOperation.getAndSet(Optional.empty());
            if (actionNameAndDuration.isPresent()) {
                ActionNameAndDuration presentActionNameAndDuration = actionNameAndDuration.get();
                log.info("Call to TimeLockService#{} took {}. This was the slowest operation that completed"
                                + " in the last {}.",
                        SafeArg.of("actionName", presentActionNameAndDuration.actionName()),
                        SafeArg.of("duration", presentActionNameAndDuration.duration()),
                        SafeArg.of("timeWindow", LOGGING_TIME_WINDOW));
            }

        }
    }

    private boolean stopwatchDescribesSlowOperation(Stopwatch stopwatch) {
        return stopwatch.elapsed().compareTo(SLOW_THRESHOLD) >= 0;
    }

    @Override
    public void close() throws Exception {
        if (delegate instanceof AutoCloseable) {
            ((AutoCloseable) delegate).close();
        }
    }

    @Value.Immutable
    interface ActionNameAndDuration {
        @Value.Parameter
        String actionName();
        @Value.Parameter
        Duration duration();
    }
}
