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

package com.palantir.timelock.paxos;

import com.palantir.atlasdb.timelock.AsyncTimelockService;
import com.palantir.lock.client.CommitTimestampGetter;
import com.palantir.lock.client.LockLeaseService;
import com.palantir.lock.client.TransactionStarter;
import com.palantir.lock.v2.ClientLockingOptions;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionResponse;
import com.palantir.lock.v2.TimelockService;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.lock.v2.WaitForLocksResponse;
import com.palantir.timestamp.TimestampRange;
import java.util.List;
import java.util.Set;

final class DelegatingTimelockService implements TimelockService {
    private final AsyncTimelockService timelock;
    private final TransactionStarter transactionStarter;
    private final LockLeaseService lockLeaseService;
    private final CommitTimestampGetter commitTimestampGetter;

    public DelegatingTimelockService(
            AsyncTimelockService timelock,
            TransactionStarter transactionStarter,
            LockLeaseService lockLeaseService,
            CommitTimestampGetter commitTimestampGetter) {
        this.timelock = timelock;
        this.transactionStarter = transactionStarter;
        this.lockLeaseService = lockLeaseService;
        this.commitTimestampGetter = commitTimestampGetter;
    }

    @Override
    public long getFreshTimestamp() {
        return timelock.getFreshTimestamp();
    }

    @Override
    public List<StartIdentifiedAtlasDbTransactionResponse> startIdentifiedAtlasDbTransactionBatch(int count) {
        return transactionStarter.startIdentifiedAtlasDbTransactionBatch(count);
    }

    @Override
    public long getCommitTimestamp(long startTs, LockToken commitLocksToken) {
        return commitTimestampGetter.getCommitTimestamp(startTs, commitLocksToken);
    }

    @Override
    public TimestampRange getFreshTimestamps(int numTimestampsRequested) {
        return timelock.getFreshTimestamps(numTimestampsRequested);
    }

    @Override
    public LockImmutableTimestampResponse lockImmutableTimestamp() {
        return lockLeaseService.lockImmutableTimestamp();
    }

    @Override
    public long getImmutableTimestamp() {
        return timelock.getImmutableTimestamp();
    }

    @Override
    public LockResponse lock(LockRequest request) {
        return lockLeaseService.lock(request);
    }

    @Override
    public LockResponse lock(LockRequest lockRequest, ClientLockingOptions options) {
        return lock(lockRequest);
    }

    @Override
    public WaitForLocksResponse waitForLocks(WaitForLocksRequest request) {
        return lockLeaseService.waitForLocks(request);
    }

    @Override
    public Set<LockToken> refreshLockLeases(Set<LockToken> tokens) {
        return transactionStarter.refreshLockLeases(tokens);
    }

    @Override
    public Set<LockToken> unlock(Set<LockToken> tokens) {
        return transactionStarter.unlock(tokens);
    }

    @Override
    public void tryUnlock(Set<LockToken> tokens) {
        unlock(tokens);
    }

    @Override
    public long currentTimeMillis() {
        return timelock.currentTimeMillis();
    }
}
