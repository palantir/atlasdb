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
import com.palantir.lock.TimeDuration;
import com.palantir.lock.impl.LockServiceImpl;
import java.util.List;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import net.bytebuddy.build.Plugin.Factory.Simple;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

@State(Scope.Benchmark)
public class LockServiceBenchmarkState {
    @Param({"true", "false"})
    public boolean collectThreadInfo;

    @Param("20")
    public int locksPerRequest;

    @Param({"5"})
    public int sleepMs;

    @Param({"2"})
    public int refreshCount;

    private Supplier<LockRequest> lockRequestSupplier;

    public Random rand = new Random();

    private ThreadAwareCloseableLockService lockService;

    @Param({"1000000", "10000"})
    public int numHeldLocksAtBeginning;

    @Param("10000")
    public int numAvailableLocks;

    @Param({"100"})
    public int threadInfoSnapshotIntervalMillis;

    private int maxBlockingDurationMillis = 100;

    @Param({"nonBlocking_asManyAsPossible", "allRandom"})
    public String requestSupplierMethodName;

    private List<LockDescriptor> heldLocksAtBeginning;

    private List<LockDescriptor> availableLocks;

    @SuppressWarnings("UnusedMethod")
    private LockRequest nonBlocking_asManyAsPossible_randomMode() {
        SortedMap<LockDescriptor, LockMode> locks = new TreeMap<>();
        for (int i = 0; i < locksPerRequest; i++) {
            locks.put(
                    availableLocks
                            .get(rand.nextInt(getAvailableLocks().size())),
                    rand.nextBoolean() ? LockMode.READ : LockMode.WRITE);
        }
        return LockRequest.builder(locks).lockAsManyAsPossible().doNotBlock().build();
    }

    private LockRequest allRandom() {
        SortedMap<LockDescriptor, LockMode> locks = new TreeMap<>();
        final int numLocks = Math.max(1, rand.nextInt(locksPerRequest));
        for (int i = 0; i < numLocks; i++) {
            locks.put(
                    availableLocks
                            .get(rand.nextInt(getAvailableLocks().size())),
                    rand.nextBoolean() ? LockMode.READ : LockMode.WRITE);
        }
        LockRequest.Builder builder = LockRequest.builder(locks);
        if (rand.nextBoolean()) {
            builder.lockAsManyAsPossible();
        }
        if (rand.nextBoolean()) {
            builder.doNotBlock();
        } else {
            builder.blockForAtMost(SimpleTimeDuration.of(rand.nextInt(maxBlockingDurationMillis), TimeUnit.MILLISECONDS));
        }
        return builder.build();
    }

    @Setup
    public void setup() {
        this.heldLocksAtBeginning = IntStream.range(0, numHeldLocksAtBeginning)
                .mapToObj(i -> StringLockDescriptor.of(Integer.toString(i)))
                .collect(Collectors.toList());
        this.availableLocks = IntStream.range(0, numAvailableLocks)
                .mapToObj(i -> StringLockDescriptor.of(Integer.toString(i)))
                .collect(Collectors.toUnmodifiableList());

        this.lockRequestSupplier = getRequestSupplierFromName(this.requestSupplierMethodName);
        this.lockService = LockServiceImpl.create(LockServerOptions.builder()
                .collectThreadInfo(collectThreadInfo)
                .threadInfoSnapshotInterval(
                        SimpleTimeDuration.of(threadInfoSnapshotIntervalMillis, TimeUnit.MILLISECONDS))
                .isStandaloneServer(false)
                .build());

        SortedMap<LockDescriptor, LockMode> locks = new TreeMap<>();
        for (LockDescriptor lock : heldLocksAtBeginning) {
            locks.put(lock, LockMode.WRITE);
        }
        LockRequest lockRequest =
                LockRequest.builder(locks).lockAsManyAsPossible().doNotBlock().build();
        try {
            lockService.lockWithFullLockResponse(LockClient.ANONYMOUS, lockRequest);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
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

    private Supplier<LockRequest> getRequestSupplierFromName(String name) {
        switch (this.requestSupplierMethodName) {
            case "nonBlocking_asManyAsPossible":
                return this::nonBlocking_asManyAsPossible_randomMode;
            case "allRandom":
                return this::allRandom;
            default:
                throw new IllegalArgumentException(name + " is not a known request supplier");
        }
    }
}
