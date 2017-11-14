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

package com.palantir.lock.client;

import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.palantir.common.base.Throwables;
import com.palantir.lock.v2.LockImmutableTimestampRequest;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.lock.v2.WaitForLocksResponse;
import com.palantir.timestamp.TimestampRange;

// TODO(nziebart): probably should make it more obvious that this class should always be used;
// maybe call this a TimelockClient and require that everywhere? Could also be used for async unlocking..
public class LockRefreshingTimelockService implements AutoCloseable, TimelockService {

    private static final long REFRESH_INTERVAL_MILLIS = 5_000;

    private final TimelockService delegate;
    private final LockRefresher lockRefresher;

    public static LockRefreshingTimelockService createDefault(TimelockService timelockService) {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
                .setNameFormat(LockRefreshingTimelockService.class.getSimpleName() + "-%d")
                .setDaemon(true)
                .build());
        LockRefresher lockRefresher = new LockRefresher(executor, timelockService, REFRESH_INTERVAL_MILLIS);
        return new LockRefreshingTimelockService(timelockService, lockRefresher);
    }

    public LockRefreshingTimelockService(TimelockService delegate, LockRefresher lockRefresher) {
        this.delegate = delegate;
        this.lockRefresher = lockRefresher;
    }

    @Override
    public boolean isInitialized() {
        return delegate.isInitialized();
    }

    @Override
    public long getFreshTimestamp() {
        try {
            return delegate.getFreshTimestamp();
        } catch (Exception e) {
            throw Throwables.unwrapAndThrowDependencyUnavailableException(e);
        }
    }

    @Override
    public TimestampRange getFreshTimestamps(int numTimestampsRequested) {
        try {
            return delegate.getFreshTimestamps(numTimestampsRequested);
        } catch (Exception e) {
            throw Throwables.unwrapAndThrowDependencyUnavailableException(e);
        }
    }

    @Override
    public LockImmutableTimestampResponse lockImmutableTimestamp(LockImmutableTimestampRequest request) {
        LockImmutableTimestampResponse response = wrapOrLockImmutableTimestamp(request);
        lockRefresher.registerLock(response.getLock());
        return response;
    }

    private LockImmutableTimestampResponse wrapOrLockImmutableTimestamp(LockImmutableTimestampRequest request) {
        try {
            return delegate.lockImmutableTimestamp(request);
        } catch (Exception e) {
            throw Throwables.unwrapAndThrowDependencyUnavailableException(e);
        }
    }

    @Override
    public long getImmutableTimestamp() {
        try {
            return delegate.getImmutableTimestamp();
        } catch (Exception e) {
            throw Throwables.unwrapAndThrowDependencyUnavailableException(e);
        }
    }

    @Override
    public LockResponse lock(LockRequest request) {
        LockResponse response = wrapOrLock(request);
        if (response.wasSuccessful()) {
            lockRefresher.registerLock(response.getToken());
        }
        return response;
    }

    private LockResponse wrapOrLock(LockRequest request) {
        try {
            return delegate.lock(request);
        } catch (Exception e) {

            throw Throwables.unwrapAndThrowDependencyUnavailableException(e);
        }
    }

    @Override
    public WaitForLocksResponse waitForLocks(WaitForLocksRequest request) {
        try {
            return delegate.waitForLocks(request);
        } catch (Exception e) {
            throw Throwables.unwrapAndThrowDependencyUnavailableException(e);
        }
    }

    @Override
    public Set<LockToken> refreshLockLeases(Set<LockToken> tokens) {
        try {
            return delegate.refreshLockLeases(tokens);
        } catch (Exception e) {
            throw Throwables.unwrapAndThrowDependencyUnavailableException(e);
        }
    }

    @Override
    public Set<LockToken> unlock(Set<LockToken> tokens) {
        lockRefresher.unregisterLocks(tokens);
        try {
            return delegate.unlock(tokens);
        } catch (Exception e) {
            throw Throwables.unwrapAndThrowDependencyUnavailableException(e);
        }
    }

    @Override
    public long currentTimeMillis() {
        try {
            return delegate.currentTimeMillis();
        } catch (Exception e) {
            throw Throwables.unwrapAndThrowDependencyUnavailableException(e);
        }
    }

    @Override
    public void close() {
        lockRefresher.close();
    }
}
