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
package com.palantir.atlasdb.sweep;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.RemoteLockService;
import com.palantir.lock.SimpleTimeDuration;
import com.palantir.lock.StringLockDescriptor;

class SweepLocks implements AutoCloseable {
    private final RemoteLockService lockService;
    private final int numberOfParallelSweeps;
    private final AtomicInteger numberOfSweepsRunning;

    private LockRefreshToken token = null;
    private Logger log = LoggerFactory.getLogger(SweepLocks.class);

    SweepLocks(RemoteLockService lockService, int numberOfParallelSweeps, AtomicInteger numberOfSweepsRunning) {
        this.lockService = lockService;
        this.numberOfParallelSweeps = numberOfParallelSweeps;
        this.numberOfSweepsRunning = numberOfSweepsRunning;
    }

    void lockOrRefresh() throws InterruptedException {
        if (token != null) {
            Set<LockRefreshToken> refreshedTokens = lockService.refreshLockRefreshTokens(ImmutableList.of(token));
            if (refreshedTokens.isEmpty()) {
                token = null;
            }
        } else {
            LockDescriptor lock = StringLockDescriptor.of("atlas sweep");
            LockRequest request = LockRequest.builder(
                    ImmutableSortedMap.of(lock, LockMode.WRITE)).doNotBlock().build();
            token = lockService.lock(LockClient.ANONYMOUS.getClientId(), request);
        }
    }

    boolean haveLocks() {
        return token != null;
    }

    @Override
    public void close() {
        if (token != null) {
            lockService.unlock(token);
        }
    }

    private LockRefreshToken sweepLeaseToken = null;

    boolean lockOrRefreshSweepLease() throws InterruptedException {
        sweepLeaseToken = lockOrRefreshSweepLeaseInternal(sweepLeaseToken);
        return sweepLeaseToken != null;
    }

    private LockRefreshToken lockOrRefreshSweepLeaseInternal(LockRefreshToken possibleToken)
            throws InterruptedException {
        if (possibleToken != null) {
            Set<LockRefreshToken> refreshedTokens = lockService.refreshLockRefreshTokens(ImmutableList.of(
                    possibleToken));

            if (!refreshedTokens.isEmpty()) {
                return possibleToken;
            }
        }

        List<Integer> possibleGrants = new ArrayList<>(numberOfParallelSweeps);
        for (int i = 1; i <= numberOfParallelSweeps; i++) {
            possibleGrants.add(i);
        }
        Collections.shuffle(possibleGrants);

        for (int i : possibleGrants) {
            LockDescriptor lock = StringLockDescriptor.of("sweep" + i);
            LockRequest request = LockRequest.builder(ImmutableSortedMap.of(lock, LockMode.WRITE))
                    .doNotBlock()
                    .timeoutAfter(SimpleTimeDuration.of(10, TimeUnit.MINUTES))
                    .build();
            LockRefreshToken possibleGrant = lockService.lock(LockClient.ANONYMOUS.getClientId(), request);

            if (possibleGrant != null) {
                numberOfSweepsRunning.incrementAndGet();
                return possibleGrant;
            }
        }

        return null;
    }

    void unlockSweepLease() {
        lockService.unlock(sweepLeaseToken);
        numberOfSweepsRunning.decrementAndGet();
        sweepLeaseToken = null;
    }

    private LockRefreshToken sweepTableToken = null;

    boolean lockTableToSweep(TableReference tableRef) throws InterruptedException {
        LockDescriptor lock = StringLockDescriptor.of("sweep-" + tableRef.getQualifiedName());
        LockRequest request = LockRequest.builder(ImmutableSortedMap.of(lock, LockMode.WRITE))
                .doNotBlock()
                .timeoutAfter(SimpleTimeDuration.of(10, TimeUnit.MINUTES))
                .build();
        sweepTableToken = lockService.lock(LockClient.ANONYMOUS.getClientId(), request);
        return sweepTableToken != null;
    }

    void unlockTableToSweep() {
        if (sweepTableToken != null) {
            lockService.unlock(sweepTableToken);
        }
        sweepTableToken = null;
    }
}
