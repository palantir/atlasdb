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

package com.palantir.atlasdb.timelock.benchmarks;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.RemoteLockService;
import com.palantir.lock.StringLockDescriptor;

public class LockAndUnlockContendedBenchmark extends AbstractBenchmark {
    private final RemoteLockService lockService;
    private final List<LockRequest> lockRequests;
    private final AtomicLong counter = new AtomicLong(0);

    public static Map<String, Object> execute(SerializableTransactionManager txnManager, int numClients,
            int requestsPerClient, int numDistinctLocks) {
        return new LockAndUnlockContendedBenchmark(txnManager.getLockService(), numClients, requestsPerClient,
                numDistinctLocks).execute();
    }

    protected LockAndUnlockContendedBenchmark(RemoteLockService lockService, int numClients, int numRequestsPerClient,
            int numDistinctLocks) {
        super(numClients, numRequestsPerClient);
        this.lockService = lockService;

        List<LockRequest> requests = Lists.newArrayList();
        for (int i = 0; i < numDistinctLocks; i++) {
            requests.add(LockRequest.builder(
                    ImmutableSortedMap.of(StringLockDescriptor.of(UUID.randomUUID().toString()),
                            LockMode.WRITE))
                    .build());
        }
        lockRequests = ImmutableList.copyOf(requests);
    }

    @Override
    protected void performOneCall() {
        try {
            LockRefreshToken token = lockService.lock(Long.toString(counter.incrementAndGet()), nextRequest());
            boolean wasUnlocked = lockService.unlock(token);
            Preconditions.checkState(wasUnlocked, "unlock returned false");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Map<String, Object> getExtraParameters() {
        return ImmutableMap.of("numDistinctLocks", lockRequests.size());
    }

    private LockRequest nextRequest() {
        return lockRequests.get((int) (counter.incrementAndGet() % lockRequests.size()));
    }
}
