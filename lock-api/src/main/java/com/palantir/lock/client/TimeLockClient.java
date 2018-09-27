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
package com.palantir.lock.client;

import java.net.ConnectException;
import java.net.UnknownHostException;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.palantir.common.base.Throwables;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.leader.NotCurrentLeaderException;
import com.palantir.lock.v2.IdentifiedTimeLockRequest;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.StartAtlasDbTransactionRequest;
import com.palantir.lock.v2.StartAtlasDbTransactionResponse;
import com.palantir.lock.v2.TimelockService;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.lock.v2.WaitForLocksResponse;
import com.palantir.timestamp.CloseableTimestampService;
import com.palantir.timestamp.RequestBatchingTimestampService;
import com.palantir.timestamp.TimestampRange;

public class TimeLockClient implements AutoCloseable, TimelockService {

    private static final long REFRESH_INTERVAL_MILLIS = 5_000;

    private final TimelockService delegate;
    private final CloseableTimestampService timestampService;
    private final LockRefresher lockRefresher;
    private final TimeLockUnlocker unlocker;

    public static TimeLockClient createDefault(TimelockService timelockService) {
        AsyncTimeLockUnlocker asyncUnlocker = AsyncTimeLockUnlocker.create(timelockService);
        RequestBatchingTimestampService timestampService =
                RequestBatchingTimestampService.create(new TimelockServiceErrorDecorator(timelockService));
        return new TimeLockClient(
                timelockService, timestampService, createLockRefresher(timelockService), asyncUnlocker);
    }

    public static TimeLockClient withSynchronousUnlocker(TimelockService timelockService) {
        CloseableTimestampService timestampService = new TimelockServiceErrorDecorator(timelockService);
        return new TimeLockClient(
                timelockService, timestampService, createLockRefresher(timelockService), timelockService::unlock);
    }

    @VisibleForTesting
    TimeLockClient(TimelockService delegate, CloseableTimestampService timestampService,
            LockRefresher lockRefresher, TimeLockUnlocker unlocker) {
        this.delegate = delegate;
        this.timestampService = timestampService;
        this.lockRefresher = lockRefresher;
        this.unlocker = unlocker;
    }

    @Override
    public boolean isInitialized() {
        return delegate.isInitialized() && timestampService.isInitialized();
    }

    @Override
    public long getFreshTimestamp() {
        return timestampService.getFreshTimestamp();
    }

    @Override
    public TimestampRange getFreshTimestamps(int numTimestampsRequested) {
        return timestampService.getFreshTimestamps(numTimestampsRequested);
    }

    @Override
    public LockImmutableTimestampResponse lockImmutableTimestamp(IdentifiedTimeLockRequest request) {
        LockImmutableTimestampResponse response = executeOnTimeLock(() -> delegate.lockImmutableTimestamp(request));
        lockRefresher.registerLock(response.getLock());
        return response;
    }

    @Override
    public StartAtlasDbTransactionResponse startAtlasDbTransaction(IdentifiedTimeLockRequest request) {
        StartAtlasDbTransactionResponse response = executeOnTimeLock(() -> delegate.startAtlasDbTransaction(request));
        lockRefresher.registerLock(response.immutableTimestamp().getLock());
        return response;
    }

    @Override
    public StartAtlasDbTransactionResponse startIdentifiedAtlasDbTransaction(StartAtlasDbTransactionRequest request) {
        StartAtlasDbTransactionResponse response = executeOnTimeLock(
                () -> delegate.startIdentifiedAtlasDbTransaction(request));
        lockRefresher.registerLock(response.immutableTimestamp().getLock());
        return response;
    }

    @Override
    public long getImmutableTimestamp() {
        return executeOnTimeLock(delegate::getImmutableTimestamp);
    }

    @Override
    public LockResponse lock(LockRequest request) {
        LockResponse response = executeOnTimeLock(() -> delegate.lock(request));
        if (response.wasSuccessful()) {
            lockRefresher.registerLock(response.getToken());
        }
        return response;
    }

    @Override
    public WaitForLocksResponse waitForLocks(WaitForLocksRequest request) {
        return executeOnTimeLock(() -> delegate.waitForLocks(request));
    }

    @Override
    public Set<LockToken> refreshLockLeases(Set<LockToken> tokens) {
        return executeOnTimeLock(() -> delegate.refreshLockLeases(tokens));
    }

    @Override
    public Set<LockToken> unlock(Set<LockToken> tokens) {
        lockRefresher.unregisterLocks(tokens);
        return executeOnTimeLock(() -> delegate.unlock(tokens));
    }

    @Override
    public void tryUnlock(Set<LockToken> tokens) {
        lockRefresher.unregisterLocks(tokens);
        unlocker.enqueue(tokens);
    }

    @Override
    public long currentTimeMillis() {
        return executeOnTimeLock(delegate::currentTimeMillis);
    }

    private static <T> T executeOnTimeLock(Callable<T> callable) {
        try {
            return callable.call();
        } catch (Exception e) {
            if (e.getCause() instanceof ConnectException
                    || e.getCause() instanceof UnknownHostException
                    || e.getCause() instanceof NotCurrentLeaderException) {
                throw Throwables.unwrapAndThrowAtlasDbDependencyException(e);
            } else {
                throw Throwables.throwUncheckedException(e);
            }
        }
    }

    @Override
    public void close() {
        lockRefresher.close();
        unlocker.close();
    }

    private static LockRefresher createLockRefresher(TimelockService timelockService) {
        ScheduledExecutorService refreshExecutor = createSingleThreadScheduledExecutor("refresh");
        return new LockRefresher(refreshExecutor, timelockService, REFRESH_INTERVAL_MILLIS);
    }

    private static ScheduledExecutorService createSingleThreadScheduledExecutor(String operation) {
        return PTExecutors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder()
                        .setNameFormat(TimeLockClient.class.getSimpleName() + "-" + operation + "-%d")
                        .setDaemon(true)
                        .build());
    }

    private static final class TimelockServiceErrorDecorator implements CloseableTimestampService {
        private final TimelockService delegate;

        private TimelockServiceErrorDecorator(TimelockService delegate) {
            this.delegate = delegate;
        }

        @Override
        public long getFreshTimestamp() {
            return executeOnTimeLock(delegate::getFreshTimestamp);
        }

        @Override
        public TimestampRange getFreshTimestamps(int numTimestampsRequested) {
            return executeOnTimeLock(() -> delegate.getFreshTimestamps(numTimestampsRequested));
        }

        @Override
        public void close() {}
    }
}
