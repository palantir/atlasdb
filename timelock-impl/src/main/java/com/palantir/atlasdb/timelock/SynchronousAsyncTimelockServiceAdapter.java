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

package com.palantir.atlasdb.timelock;

import java.util.Set;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.QueryParam;

import com.palantir.atlasdb.timelock.lock.AsyncResult;
import com.palantir.atlasdb.timelock.paxos.ManagedTimestampService;
import com.palantir.lock.LockClient;
import com.palantir.lock.RemoteLockService;
import com.palantir.lock.impl.LegacyTimelockService;
import com.palantir.lock.v2.LockImmutableTimestampRequest;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockRequestV2;
import com.palantir.lock.v2.LockTokenV2;
import com.palantir.lock.v2.TimelockService;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.timestamp.TimestampManagementService;
import com.palantir.timestamp.TimestampRange;

public class SynchronousAsyncTimelockServiceAdapter implements AsyncTimelockService {
    private final TimelockService timelockService;
    private final TimestampManagementService timestampManagementService;

    private static final LockClient LEGACY_LOCK_CLIENT = LockClient.of("legacy");

    private SynchronousAsyncTimelockServiceAdapter(
            TimelockService timelockService,
            TimestampManagementService timestampManagementService) {
        this.timelockService = timelockService;
        this.timestampManagementService = timestampManagementService;
    }

    public static SynchronousAsyncTimelockServiceAdapter createLegacyBackedService(
            ManagedTimestampService managedTimestampService,
            RemoteLockService remoteLockService) {
        return new SynchronousAsyncTimelockServiceAdapter(
                new LegacyTimelockService(managedTimestampService, remoteLockService, LEGACY_LOCK_CLIENT),
                managedTimestampService);
    }

    @Override
    public long currentTimeMillis() {
        return timelockService.currentTimeMillis();
    }

    @Override
    public Set<LockTokenV2> unlock(Set<LockTokenV2> tokens) {
        return timelockService.unlock(tokens);
    }

    @Override
    public Set<LockTokenV2> refreshLockLeases(Set<LockTokenV2> tokens) {
        return timelockService.refreshLockLeases(tokens);
    }

    @Override
    public AsyncResult<Void> waitForLocks(WaitForLocksRequest request) {
        timelockService.waitForLocks(request);
        return AsyncResult.completedResult();
    }

    @Override
    public AsyncResult<LockTokenV2> lock(LockRequestV2 request) {
        AsyncResult<LockTokenV2> result = new AsyncResult<>();
        result.complete(timelockService.lock(request).getToken());
        return result;
    }

    @Override
    public long getImmutableTimestamp() {
        return timelockService.getImmutableTimestamp();
    }

    @Override
    public LockImmutableTimestampResponse lockImmutableTimestamp(LockImmutableTimestampRequest request) {
        return timelockService.lockImmutableTimestamp(request);
    }

    @Override
    public long getFreshTimestamp() {
        return timelockService.getFreshTimestamp();
    }

    @Override
    public TimestampRange getFreshTimestamps(@QueryParam("number") int numTimestampsRequested) {
        return timelockService.getFreshTimestamps(numTimestampsRequested);
    }

    @Override
    public void fastForwardTimestamp(
            @QueryParam("currentTimestamp") @DefaultValue(SENTINEL_TIMESTAMP_STRING) long currentTimestamp) {
        timestampManagementService.fastForwardTimestamp(currentTimestamp);
    }

    @Override
    public String ping() {
        return timestampManagementService.ping();
    }
}
