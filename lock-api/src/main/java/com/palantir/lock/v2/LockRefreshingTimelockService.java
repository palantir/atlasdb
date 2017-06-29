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

package com.palantir.lock.v2;

import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.palantir.lock.LockRefreshToken;
import com.palantir.timestamp.TimestampRange;

public class LockRefreshingTimelockService implements TimelockService {

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
    public LockRefreshToken lock(LockRequestV2 request) {
        LockRefreshToken response = delegate.lock(request);
        lockRefresher.registerLock(response);
        return response;
    }

    @Override
    public void waitForLocks(WaitForLocksRequest request) {
        delegate.waitForLocks(request);
    }

    @Override
    public Set<LockRefreshToken> refreshLockLeases(Set<LockRefreshToken> tokens) {
        return delegate.refreshLockLeases(tokens);
    }

    @Override
    public Set<LockRefreshToken> unlock(Set<LockRefreshToken> tokens) {
        lockRefresher.unregisterLocks(tokens);
        return delegate.unlock(tokens);
    }

    @Override
    public long currentTimeMillis() {
        return delegate.currentTimeMillis();
    }
}
