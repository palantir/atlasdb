/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.lock.client;

import java.net.ConnectException;
import java.net.UnknownHostException;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.palantir.common.base.Throwables;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.leader.NotCurrentLeaderException;
import com.palantir.lock.v2.LockImmutableTimestampRequest;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.lock.v2.WaitForLocksResponse;
import com.palantir.timestamp.TimestampRange;

public class TimeLockClient implements AutoCloseable, TimelockService {

    private static final long REFRESH_INTERVAL_MILLIS = 5_000;

    private final TimelockService delegate;
    private final LockRefresher lockRefresher;

    public static TimeLockClient createDefault(TimelockService timelockService) {
        ScheduledExecutorService executor = PTExecutors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
                .setNameFormat(TimeLockClient.class.getSimpleName() + "-%d")
                .setDaemon(true)
                .build());
        LockRefresher lockRefresher = new LockRefresher(executor, timelockService, REFRESH_INTERVAL_MILLIS);
        return new TimeLockClient(timelockService, lockRefresher);
    }

    public TimeLockClient(TimelockService delegate, LockRefresher lockRefresher) {
        this.delegate = delegate;
        this.lockRefresher = lockRefresher;
    }

    @Override
    public boolean isInitialized() {
        return delegate.isInitialized();
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
    public LockImmutableTimestampResponse lockImmutableTimestamp(LockImmutableTimestampRequest request) {
        LockImmutableTimestampResponse response = executeOnTimeLock(() -> delegate.lockImmutableTimestamp(request));
        lockRefresher.registerLock(response.getLock());
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
    public long currentTimeMillis() {
        return executeOnTimeLock(delegate::currentTimeMillis);
    }

    private <T> T executeOnTimeLock(Callable<T> callable) {
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
    }
}
