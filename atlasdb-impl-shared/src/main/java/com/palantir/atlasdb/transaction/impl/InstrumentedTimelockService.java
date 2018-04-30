/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
import java.util.function.Supplier;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.palantir.atlasdb.AtlasDbMetricNames;
import com.palantir.lock.v2.LockImmutableTimestampRequest;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.lock.v2.WaitForLocksResponse;
import com.palantir.timestamp.TimestampRange;

public class InstrumentedTimelockService implements TimelockService {
    private final TimelockService timelockService;
    private final Meter success;
    private final Meter fail;

    public InstrumentedTimelockService(TimelockService timelockService, MetricRegistry metricRegistry) {
        this.timelockService = timelockService;
        this.success = metricRegistry.meter(AtlasDbMetricNames.TIMELOCK_SUCCESSFUL_REQUEST);
        this.fail = metricRegistry.meter(AtlasDbMetricNames.TIMELOCK_FAILED_REQUEST);
    }

    @Override
    public boolean isInitialized() {
        return timelockService.isInitialized();
    }

    @Override
    public long getFreshTimestamp() {
        return executeWithRecord(() -> timelockService.getFreshTimestamp());
    }

    @Override
    public TimestampRange getFreshTimestamps(int numTimestampsRequested) {
        return executeWithRecord(() -> timelockService.getFreshTimestamps(numTimestampsRequested));
    }

    @Override
    public LockImmutableTimestampResponse lockImmutableTimestamp(
            LockImmutableTimestampRequest request) {
        return executeWithRecord(() -> timelockService.lockImmutableTimestamp(request));
    }

    @Override
    public long getImmutableTimestamp() {
        return executeWithRecord(() -> timelockService.getImmutableTimestamp());
    }

    @Override
    public LockResponse lock(LockRequest request) {
        return executeWithRecord(() -> timelockService.lock(request));
    }

    @Override
    public WaitForLocksResponse waitForLocks(WaitForLocksRequest request) {
        return executeWithRecord(() -> timelockService.waitForLocks(request));
    }

    @Override
    public Set<LockToken> refreshLockLeases(
            Set<LockToken> tokens) {
        return executeWithRecord(() -> timelockService.refreshLockLeases(tokens));
    }

    @Override
    public Set<LockToken> unlock(Set<LockToken> tokens) {
        return executeWithRecord(() -> timelockService.unlock(tokens));
    }

    @Override
    public long currentTimeMillis() {
        return executeWithRecord(() -> timelockService.currentTimeMillis());
    }

    private <T> T executeWithRecord(Supplier<T> method) {
        try {
            T t = method.get();
            success.mark();
            return t;
        } catch (Exception e) {
            fail.mark();
            throw e;
        }
    }
}
