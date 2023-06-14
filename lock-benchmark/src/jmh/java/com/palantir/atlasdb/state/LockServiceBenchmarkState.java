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

import com.palantir.lock.ImmutableDebugThreadInfoConfiguration;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.SimpleTimeDuration;
import com.palantir.lock.StringLockDescriptor;
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
    public boolean recordThreadInfo;

    @Param({"20"})
    public int locksPerRequest;

    @Param({"5", "0"})
    public int sleepMs;

    @Param({"0"})
    public int refreshCount;

    @Param({"1000000"})
    public int numHeldLocksAtBeginning;

    @Param({"1000000"})
    public int numAvailableLocks;

    @Param({"100"})
    public long threadInfoSnapshotIntervalMillis;

    @Param({"nonBlockingAsManyAsPossibleRandomMode"})
    public String requestSupplierMethodName;

    private final int maxBlockingDurationMillis = 100;
    private final Random rand = new Random();
    private LockServiceImpl lockService;
    private Supplier<LockRequest> lockRequestSupplier;
    private List<LockDescriptor> availableLocks;

    private LockRequest nonBlockingAsManyAsPossibleRandomMode() {
        return LockRequest.builder(getRandomLocksWithRandomMode(locksPerRequest))
                .lockAsManyAsPossible()
                .doNotBlock()
                .build();
    }

    private LockRequest allRandom() {
        LockRequest.Builder builder =
                LockRequest.builder(getRandomLocksWithRandomMode(Math.max(1, rand.nextInt(locksPerRequest))));
        if (rand.nextBoolean()) {
            builder.lockAsManyAsPossible();
        }
        // Never choose BLOCK_INDEFINITELY since it is incompatible with LOCK_AS_MANY_AS_POSSIBLE
        if (rand.nextBoolean()) {
            builder.doNotBlock();
        } else {
            builder.blockForAtMost(
                    SimpleTimeDuration.of(rand.nextInt(maxBlockingDurationMillis), TimeUnit.MILLISECONDS));
        }
        return builder.build();
    }

    @Setup
    public void setup() throws InterruptedException {
        availableLocks = IntStream.range(0, numAvailableLocks)
                .mapToObj(i -> StringLockDescriptor.of(Integer.toString(i)))
                .collect(Collectors.toUnmodifiableList());
        lockRequestSupplier = getRequestSupplierFromName(requestSupplierMethodName);
        lockService = LockServiceImpl.create(LockServerOptions.builder()
                .threadInfoConfiguration(ImmutableDebugThreadInfoConfiguration.builder()
                        .recordThreadInfo(recordThreadInfo)
                        .threadInfoSnapshotIntervalMillis(threadInfoSnapshotIntervalMillis)
                        .build())
                .isStandaloneServer(false)
                .build());
        // Simulate locks that are permanently locked to saturate heldLocksTokenMap
        lockService.lockWithFullLockResponse(
                LockClient.ANONYMOUS,
                LockRequest.builder(getRandomLocksWithRandomMode(numHeldLocksAtBeginning))
                        .lockAsManyAsPossible()
                        .doNotBlock()
                        .build());
    }

    private SortedMap<LockDescriptor, LockMode> getRandomLocksWithRandomMode(int numLocks) {
        SortedMap<LockDescriptor, LockMode> locks = new TreeMap<>();
        IntStream.range(0, numLocks)
                .forEach(i -> locks.put(
                        availableLocks.get(rand.nextInt(availableLocks.size())),
                        rand.nextBoolean() ? LockMode.READ : LockMode.WRITE));
        return locks;
    }

    public LockServiceImpl getLockService() {
        return lockService;
    }

    public List<LockDescriptor> getAvailableLocks() {
        return availableLocks;
    }

    public LockRequest generateLockRequest() {
        return lockRequestSupplier.get();
    }

    private Supplier<LockRequest> getRequestSupplierFromName(String name) {
        switch (requestSupplierMethodName) {
            case "nonBlockingAsManyAsPossibleRandomMode":
                return this::nonBlockingAsManyAsPossibleRandomMode;
            case "allRandom":
                return this::allRandom;
            default:
                throw new IllegalArgumentException(name + " is not a known request supplier");
        }
    }
}
