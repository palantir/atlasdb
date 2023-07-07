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
package com.palantir.timelock.paxos;

import com.palantir.atlasdb.timelock.lock.BlockingTimeLimitedLockService;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.lock.CloseableLockService;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.impl.LockServiceImpl;
import com.palantir.lock.impl.ThreadPooledLockService;
import com.palantir.refreshable.Refreshable;
import com.palantir.timelock.config.TimeLockRuntimeConfiguration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;

public class LockCreator {
    private final Refreshable<TimeLockRuntimeConfiguration> runtime;
    private final long blockingTimeoutMs;
    private final Semaphore sharedThreadPool;
    private final ExecutorService sharedExecutor = PTExecutors.newCachedThreadPool(LockServiceImpl.class.getName());

    public LockCreator(Refreshable<TimeLockRuntimeConfiguration> runtime, int threadPoolSize, long blockingTimeoutMs) {
        this.runtime = runtime;
        this.sharedThreadPool = new Semaphore(threadPoolSize);
        this.blockingTimeoutMs = blockingTimeoutMs;
    }

    public CloseableLockService createThreadPoolingLockService() {
        Refreshable<LockServerOptions> lockServerOptions = runtime.map(rt -> LockServerOptions.builder()
                .slowLogTriggerMillis(rt.slowLockLogTriggerMillis())
                .threadInfoConfiguration(rt.threadInfoConfiguration())
                .build());

        LockServiceImpl rawLockService = LockServiceImpl.create(lockServerOptions, sharedExecutor);
        CloseableLockService lockService = BlockingTimeLimitedLockService.create(
                rawLockService, blockingTimeoutMs, rawLockService.getSnapshotManager());

        return new ThreadPooledLockService(lockService, -1, sharedThreadPool);
    }
}
