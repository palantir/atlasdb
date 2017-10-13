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

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
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
    private final Optional<ScheduledExecutorService> managedExecutor;

    /**
     * @deprecated Use {@link #create(TimelockService, ScheduledExecutorService)} instead.
     *
     * @param timelockService The {@link TimelockService} to wrap
     * @return A {@link TimelockService} that automatically refreshes locks
     */
    @Deprecated
    public static LockRefreshingTimelockService createDefault(TimelockService timelockService) {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
                .setNameFormat(LockRefreshingTimelockService.class.getSimpleName() + "-%d")
                .setDaemon(true)
                .build());
        LockRefresher lockRefresher = new LockRefresher(executor, timelockService, REFRESH_INTERVAL_MILLIS);
        return new LockRefreshingTimelockService(timelockService, lockRefresher, executor);
    }

    /**
     * Creates a {@link TimelockService} that uses the specified executor to regularly refresh
     * locks that have been taken out.
     *
     * @param timelockService The {@link TimelockService} proxy to wrap
     * @param executor An executor service whose lifecycle is managed by the consumer of this method
     * @return The {@link TimelockService} that automatically refreshes locks
     */
    public static LockRefreshingTimelockService create(
            TimelockService timelockService,
            ScheduledExecutorService executor) {
        LockRefresher lockRefresher = new LockRefresher(executor, timelockService, REFRESH_INTERVAL_MILLIS);
        return new LockRefreshingTimelockService(timelockService, lockRefresher, null);
    }

    @VisibleForTesting
    LockRefreshingTimelockService(
            TimelockService delegate,
            LockRefresher lockRefresher,
            ScheduledExecutorService executor) {
        this.delegate = delegate;
        this.lockRefresher = lockRefresher;
        this.managedExecutor = Optional.ofNullable(executor);
    }

    @Override
    public boolean isInitialized() {
        return delegate.isInitialized();
    }

    @Override
    public long getFreshTimestamp() {
        return delegate.getFreshTimestamp();
    }

    @Override
    public TimestampRange getFreshTimestamps(int numTimestampsRequested) {
        return delegate.getFreshTimestamps(numTimestampsRequested);
    }

    @Override
    public LockImmutableTimestampResponse lockImmutableTimestamp(LockImmutableTimestampRequest request) {
        LockImmutableTimestampResponse response = delegate.lockImmutableTimestamp(request);
        lockRefresher.registerLock(response.getLock());
        return response;
    }

    @Override
    public long getImmutableTimestamp() {
        return delegate.getImmutableTimestamp();
    }

    @Override
    public LockResponse lock(LockRequest request) {
        LockResponse response = delegate.lock(request);
        if (response.wasSuccessful()) {
            lockRefresher.registerLock(response.getToken());
        }
        return response;
    }

    @Override
    public WaitForLocksResponse waitForLocks(WaitForLocksRequest request) {
        return delegate.waitForLocks(request);
    }

    @Override
    public Set<LockToken> refreshLockLeases(Set<LockToken> tokens) {
        return delegate.refreshLockLeases(tokens);
    }

    @Override
    public Set<LockToken> unlock(Set<LockToken> tokens) {
        lockRefresher.unregisterLocks(tokens);
        return delegate.unlock(tokens);
    }

    @Override
    public long currentTimeMillis() {
        return delegate.currentTimeMillis();
    }

    @Override
    public void close() throws Exception {
        managedExecutor.ifPresent(ScheduledExecutorService::shutdown);
    }
}
