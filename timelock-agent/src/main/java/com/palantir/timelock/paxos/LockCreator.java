/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.timelock.paxos;

import java.util.concurrent.Semaphore;
import java.util.function.Supplier;

import com.palantir.atlasdb.timelock.lock.BlockingTimeLimitedLockService;
import com.palantir.lock.CloseableLockService;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.impl.LockServiceImpl;
import com.palantir.lock.impl.ThreadPooledLockService;
import com.palantir.timelock.config.TimeLockDeprecatedConfiguration;
import com.palantir.timelock.config.TimeLockRuntimeConfiguration;

public class LockCreator {
    private final Supplier<TimeLockRuntimeConfiguration> runtime;
    private final TimeLockDeprecatedConfiguration deprecated;

    private static Semaphore sharedThreadPool = new Semaphore(-1);

    public LockCreator(Supplier<TimeLockRuntimeConfiguration> runtime, TimeLockDeprecatedConfiguration deprecated) {
        this.runtime = runtime;
        this.deprecated = deprecated;
    }

    public CloseableLockService createThreadPoolingLockService() {
        // TODO (jkong): Live reload slow lock timeout, plus clients (issue #2342)
        // TODO (?????): Rewrite ThreadPooled to cope with live reload, and/or remove ThreadPooled (if using Async)
        TimeLockRuntimeConfiguration timeLockRuntimeConfiguration = runtime.get();
        CloseableLockService lockServiceNotUsingThreadPooling = createTimeLimitedLockService(
                timeLockRuntimeConfiguration.slowLockLogTriggerMillis());

        if (!deprecated.useClientRequestLimit()) {
            return lockServiceNotUsingThreadPooling;
        }

        int availableThreads = deprecated.availableThreads();
        // TODO(nziebart): Since the number of clients can grow dynamically, we can't compute a correct and useful
        // value for the local threadpool size at this point. Given that async lock service exists, and doesn't need
        // a thread pool, it's likely we won't fix this and will eventually remove the thread pooled lock service.
        // However, for the time being, it's still useful to have global limiting for services that need to use the
        // legacy lock service.
        int localThreadPoolSize = -1;
        int sharedThreadPoolSize = availableThreads;

        synchronized (this) {
            if (sharedThreadPool.availablePermits() == -1) {
                sharedThreadPool.release(sharedThreadPoolSize + 1);
            }
        }

        return new ThreadPooledLockService(lockServiceNotUsingThreadPooling, localThreadPoolSize, sharedThreadPool);
    }

    private CloseableLockService createTimeLimitedLockService(long slowLogTriggerMillis) {
        LockServerOptions lockServerOptions = LockServerOptions.builder()
                .slowLogTriggerMillis(slowLogTriggerMillis)
                .build();

        LockServiceImpl rawLockService = LockServiceImpl.create(lockServerOptions);

        if (deprecated.useLockTimeLimiter()) {
            return BlockingTimeLimitedLockService.create(
                    rawLockService,
                    deprecated.blockingTimeoutInMs());
        }
        return rawLockService;
    }
}
