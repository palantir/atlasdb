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

import java.io.IOException;
import java.util.Set;

import com.palantir.atlasdb.timelock.lock.AsyncLockService;
import com.palantir.atlasdb.timelock.lock.AsyncResult;
import com.palantir.atlasdb.timelock.lock.TimeLimit;
import com.palantir.atlasdb.timelock.paxos.ManagedTimestampService;
import com.palantir.lock.v2.IdentifiedTimeLockRequest;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.timestamp.TimestampRange;

public class AsyncTimelockServiceImpl implements AsyncTimelockService {

    private final AsyncLockService lockService;
    private final ManagedTimestampService timestampService;

    public AsyncTimelockServiceImpl(AsyncLockService lockService, ManagedTimestampService timestampService) {
        this.lockService = lockService;
        this.timestampService = timestampService;
    }

    @Override
    public boolean isInitialized() {
        return timestampService.isInitialized();
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
        long timestamp = timestampService.getFreshTimestamp();

        // this will always return synchronously
        LockToken token = lockService.lockImmutableTimestamp(request.getRequestId(), timestamp).get();
        long immutableTs = lockService.getImmutableTimestamp().orElse(timestamp);

        return LockImmutableTimestampResponse.of(immutableTs, token);
    }

    @Override
    public long getImmutableTimestamp() {
        long timestamp = timestampService.getFreshTimestamp();
        return lockService.getImmutableTimestamp().orElse(timestamp);
    }

    @Override
    public AsyncResult<LockToken> lock(LockRequest request) {
        return lockService.lock(
                request.getRequestId(),
                request.getLockDescriptors(),
                TimeLimit.of(request.getAcquireTimeoutMs()));
    }

    @Override
    public AsyncResult<Void> waitForLocks(WaitForLocksRequest request) {
        return lockService.waitForLocks(
                request.getRequestId(),
                request.getLockDescriptors(),
                TimeLimit.of(request.getAcquireTimeoutMs()));
    }

    @Override
    public Set<LockToken> refreshLockLeases(Set<LockToken> tokens) {
        return lockService.refresh(tokens);
    }

    @Override
    public Set<LockToken> unlock(Set<LockToken> tokens) {
        return lockService.unlock(tokens);
    }

    @Override
    public long currentTimeMillis() {
        return System.currentTimeMillis();
    }

    @Override
    public void fastForwardTimestamp(long currentTimestamp) {
        timestampService.fastForwardTimestamp(currentTimestamp);
    }

    @Override
    public String ping() {
        return timestampService.ping();
    }

    @Override
    public void close() throws IOException {
        lockService.close();
    }
}
