/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock;

import com.palantir.lock.v2.ClientLockingOptions;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionResponse;
import com.palantir.lock.v2.TimelockService;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.lock.v2.WaitForLocksResponse;
import com.palantir.timestamp.InMemoryTimestampService;
import com.palantir.timestamp.TimestampRange;
import java.util.List;
import java.util.Set;

/**
 * In-memory implementation of timelock service. This should only be used in test code.
 */
public class InMemoryTimelockService implements TimelockService {
    // use "real" production timelock - just add this layer to avoid RPC calls in testing

    private final InMemoryTimestampService timestamp;

    public InMemoryTimelockService(InMemoryTimestampService timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public long getFreshTimestamp() {
        return timestamp.getFreshTimestamp();
    }

    @Override
    public long getCommitTimestamp(long startTs, LockToken commitLocksToken) {
        return 0;
    }

    @Override
    public TimestampRange getFreshTimestamps(int numTimestampsRequested) {
        return null;
    }

    @Override
    public LockImmutableTimestampResponse lockImmutableTimestamp() {
        return null;
    }

    @Override
    public List<StartIdentifiedAtlasDbTransactionResponse> startIdentifiedAtlasDbTransactionBatch(int count) {
        return null;
    }

    @Override
    public long getImmutableTimestamp() {
        return 0;
    }

    @Override
    public LockResponse lock(LockRequest request) {
        return null;
    }

    @Override
    public LockResponse lock(LockRequest lockRequest, ClientLockingOptions options) {
        return null;
    }

    @Override
    public WaitForLocksResponse waitForLocks(WaitForLocksRequest request) {
        return null;
    }

    @Override
    public Set<LockToken> refreshLockLeases(Set<LockToken> tokens) {
        return null;
    }

    @Override
    public Set<LockToken> unlock(Set<LockToken> tokens) {
        return null;
    }

    @Override
    public void tryUnlock(Set<LockToken> tokens) {}

    @Override
    public long currentTimeMillis() {
        return 0;
    }
}
