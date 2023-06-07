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

package com.palantir.atlasdb;

import com.palantir.atlasdb.state.LockServiceBenchmarkState;
import com.palantir.atlasdb.state.ThreadIndex;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockResponse;
import com.palantir.lock.LockService;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

@SuppressWarnings("checkstyle:HideUtilityClassConstructor")
@Measurement(iterations = 2)
@Warmup(iterations = 1)
@Fork(2)
public class LockServiceBenchmark {

    @BenchmarkMode(Mode.Throughput)
    @Benchmark
    @Threads(4)
    public int lockSleepUnlockMultiThreaded(LockServiceBenchmarkState state, ThreadIndex threadIndex) {
        final LockClient client = LockClient.of("Benchmark Client " + threadIndex.getThreadId());
        final LockService lockService = state.getLockService();
        final LockRequest lockRequest = state.generateLockRequest();
        try {
            // acquire some locks
            LockResponse lockResponse = lockService.lockWithFullLockResponse(client, lockRequest);

            // pretend we are doing something with them ...
            if (state.sleepMs > 0) {
                Thread.sleep(state.sleepMs);
            }

            // give them back
            if (lockResponse.getLockRefreshToken() != null) {
                lockService.unlock(lockResponse.getLockRefreshToken());
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        return lockRequest.getLocks().size();
    }
}
