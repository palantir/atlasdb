/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.palantir.lock.v2.LockImmutableTimestampRequest;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.lock.v2.WaitForLocksResponse;
import com.palantir.timestamp.TimestampRange;

public class MetricsCollectingTimelockService implements TimelockService{
    private final TimelockService timelockService;
    private final Meter success;
    private final Meter fail;

    public MetricsCollectingTimelockService (TimelockService timelockService, MetricRegistry metricRegistry) {
        this.timelockService = timelockService;
        this.success = metricRegistry.meter("timelock.success");
        this.fail = metricRegistry.meter("timelock.fail");
    }

    @Override
    public boolean isInitialized() {
        return timelockService.isInitialized();
    }

    @Override
    public long getFreshTimestamp() {
        try {
            long freshTimestamp = timelockService.getFreshTimestamp();
            success.mark();
            return freshTimestamp;
        } catch (RuntimeException e) {
            fail.mark();
            throw e;
        }
    }

    @Override
    public TimestampRange getFreshTimestamps(int numTimestampsRequested) {
        try {
            TimestampRange timestampRange = timelockService.getFreshTimestamps(numTimestampsRequested);
            success.mark();
            return timestampRange;
        } catch (RuntimeException e) {
            fail.mark();
            throw e;
        }
    }

    @Override
    public LockImmutableTimestampResponse lockImmutableTimestamp(
            LockImmutableTimestampRequest request) {
        try {
            LockImmutableTimestampResponse response = timelockService.lockImmutableTimestamp(request);
            success.mark();
            return response;
        } catch (RuntimeException e) {
            fail.mark();
            throw e;
        }
    }

    @Override
    public long getImmutableTimestamp() {
        try {
            long immutableTimestamp = timelockService.getImmutableTimestamp();
            success.mark();
            return immutableTimestamp;
        } catch (RuntimeException e) {
            fail.mark();
            throw e;
        }
    }

    @Override
    public LockResponse lock(LockRequest request) {
        try {
            LockResponse response = timelockService.lock(request);
            success.mark();
            return response;
        } catch (RuntimeException e) {
            fail.mark();
            throw e;
        }
    }

    @Override
    public WaitForLocksResponse waitForLocks(WaitForLocksRequest request) {
        try {
            WaitForLocksResponse response = timelockService.waitForLocks(request);
            success.mark();
            return response;
        } catch (RuntimeException e) {
            fail.mark();
            throw e;
        }
    }

    @Override
    public Set<LockToken> refreshLockLeases(
            Set<LockToken> tokens) {
        try {
            Set<LockToken> lockTokens = timelockService.refreshLockLeases(tokens);
            success.mark();
            return lockTokens;
        } catch (RuntimeException e) {
            fail.mark();
            throw e;
        }
    }

    @Override
    public Set<LockToken> unlock(Set<LockToken> tokens) {
        try {
            Set<LockToken> lockTokens = unlock(tokens);
            success.mark();
            return lockTokens;
        } catch (RuntimeException e) {
            fail.mark();
            throw e;
        }
    }

    @Override
    public long currentTimeMillis() {
        try {
            long currentTime = timelockService.currentTimeMillis();
            success.mark();
            return currentTime;
        } catch (RuntimeException e) {
            fail.mark();
            throw e;
        }
    }
}
