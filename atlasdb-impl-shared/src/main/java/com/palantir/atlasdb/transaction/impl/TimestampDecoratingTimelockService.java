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

package com.palantir.atlasdb.transaction.impl;

import java.util.Set;

import com.palantir.lock.v2.LockImmutableTimestampRequest;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.lock.v2.WaitForLocksResponse;
import com.palantir.timestamp.TimestampRange;
import com.palantir.timestamp.TimestampService;

public class TimestampDecoratingTimelockService implements TimelockService {
    private final TimelockService delegate;
    private final TimestampService decoratedTimestamps;

    public TimestampDecoratingTimelockService(TimelockService delegate, TimestampService decoratedTimestamps) {
        this.delegate = delegate;
        this.decoratedTimestamps = decoratedTimestamps;
    }

    @Override
    public boolean isInitialized() {
        return delegate.isInitialized() && decoratedTimestamps.isInitialized();
    }

    @Override
    public long getFreshTimestamp() {
        return decoratedTimestamps.getFreshTimestamp();
    }

    @Override
    public TimestampRange getFreshTimestamps(int numTimestampsRequested) {
        return decoratedTimestamps.getFreshTimestamps(numTimestampsRequested);
    }

    @Override
    public LockImmutableTimestampResponse lockImmutableTimestamp(LockImmutableTimestampRequest request) {
        return delegate.lockImmutableTimestamp(request);
    }

    @Override
    public long getImmutableTimestamp() {
        return delegate.getImmutableTimestamp();
    }

    @Override
    public LockResponse lock(LockRequest request) {
        return delegate.lock(request);
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
        return delegate.unlock(tokens);
    }

    @Override
    public long currentTimeMillis() {
        return delegate.currentTimeMillis();
    }
}
