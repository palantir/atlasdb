/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.timelock.benchmarks.benchmarks;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;
import com.palantir.logsafe.Preconditions;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class LockAndUnlockContendedBenchmark extends AbstractBenchmark {
    private static final int ACQUIRE_TIMEOUT_MS = 50_000;

    private final TimelockService timelock;
    private final List<LockDescriptor> lockDescriptors;
    private final AtomicLong counter = new AtomicLong(0);

    public static Map<String, Object> execute(TransactionManager txnManager, int numClients,
            int requestsPerClient, int numDistinctLocks) {
        return new LockAndUnlockContendedBenchmark(txnManager.getTimelockService(), numClients, requestsPerClient,
                numDistinctLocks).execute();
    }

    protected LockAndUnlockContendedBenchmark(TimelockService timelock, int numClients, int numRequestsPerClient,
            int numDistinctLocks) {
        super(numClients, numRequestsPerClient);
        this.timelock = timelock;

        List<LockDescriptor> descriptors = Lists.newArrayListWithExpectedSize(numDistinctLocks);
        for (int i = 0; i < numDistinctLocks; i++) {
            descriptors.add(StringLockDescriptor.of(UUID.randomUUID().toString()));
        }
        lockDescriptors = ImmutableList.copyOf(descriptors);
    }

    @Override
    protected void performOneCall() {
        LockToken token = timelock.lock(nextRequest()).getToken();
        boolean wasUnlocked = timelock.unlock(ImmutableSet.of(token)).contains(token);
        Preconditions.checkState(wasUnlocked, "unlock returned false");
    }

    @Override
    protected Map<String, Object> getExtraParameters() {
        return ImmutableMap.of("numDistinctLocks", lockDescriptors.size());
    }

    private LockRequest nextRequest() {
        LockDescriptor lockDescriptor = lockDescriptors.get((int) (counter.incrementAndGet() % lockDescriptors.size()));
        return LockRequest.of(ImmutableSet.of(lockDescriptor), ACQUIRE_TIMEOUT_MS);
    }
}
