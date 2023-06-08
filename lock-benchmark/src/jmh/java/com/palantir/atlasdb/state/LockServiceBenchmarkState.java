/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.state;

import com.palantir.lock.LockClient;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.SimpleTimeDuration;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.ThreadAwareCloseableLockService;
import com.palantir.lock.impl.LockServiceImpl;
import java.util.List;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

@State(Scope.Benchmark)
public class LockServiceBenchmarkState {
    @Param({"true", "false"})
    public boolean collectAndLogThreadInfo;

    @Param("20")
    public int locksPerRequest;

    @Param({"50", "5", "0"})
    public int sleepMs;

    private Supplier<LockRequest> lockRequestSupplier;

    public Random rand = new Random();

    private ThreadAwareCloseableLockService lockService;

    @Param("1000000")
    public int numHeldLocksAtBeginning;

    @Param("10000")
    public int numAvailableLocks;

    private List<LockDescriptor> heldLocksAtBeginning;

    private List<LockDescriptor> availableLocks;

    private LockRequest nonBlocking_asManyAsPossible_randomMode() {
        SortedMap<LockDescriptor, LockMode> locks = new TreeMap<>();
        for (int i = 0; i < locksPerRequest; i++) {
            locks.put(
                    this.getAvailableLocks()
                            .get(rand.nextInt(getAvailableLocks().size())),
                    Math.random() > 0.5 ? LockMode.READ : LockMode.WRITE);
        }
        return LockRequest.builder(locks).lockAsManyAsPossible().doNotBlock().build();
    }

    public void lockExclusively(LockDescriptor lockDescriptor) {
        SortedMap<LockDescriptor, LockMode> locks = new TreeMap<>();
        locks.put(lockDescriptor, LockMode.WRITE);
        LockRequest lockRequest =
                LockRequest.builder(locks).lockAsManyAsPossible().doNotBlock().build();
        try {
            lockService.lockWithFullLockResponse(LockClient.ANONYMOUS, lockRequest);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Setup
    public void setup() {
        this.heldLocksAtBeginning = IntStream.range(0, numHeldLocksAtBeginning)
                .mapToObj(i -> StringLockDescriptor.of(Integer.toString(i)))
                .collect(Collectors.toList());
        this.availableLocks = IntStream.range(0, numAvailableLocks)
                .mapToObj(i -> StringLockDescriptor.of(Integer.toString(i)))
                .collect(Collectors.toUnmodifiableList());
        this.lockRequestSupplier = this::nonBlocking_asManyAsPossible_randomMode;
        this.lockService = LockServiceImpl.create(LockServerOptions.builder()
                .collectThreadInfo(collectAndLogThreadInfo)
                // We want to measure the worst-case scenario where our background task is constantly running
                .threadInfoSnapshotInterval(SimpleTimeDuration.of(0, TimeUnit.MILLISECONDS))
                .isStandaloneServer(false)
                .build());
        for (LockDescriptor lock : heldLocksAtBeginning) {
            lockExclusively(lock);
        }
    }

    public ThreadAwareCloseableLockService getLockService() {
        return this.lockService;
    }

    public List<LockDescriptor> getAvailableLocks() {
        return this.availableLocks;
    }

    public LockRequest generateLockRequest() {
        return this.lockRequestSupplier.get();
    }
}
