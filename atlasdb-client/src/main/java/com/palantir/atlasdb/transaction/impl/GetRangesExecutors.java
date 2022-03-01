/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.impl;

import com.palantir.common.concurrent.SharedFixedExecutors;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.util.RateLimitedLogger;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public final class GetRangesExecutors {
    private static final SafeLogger log = SafeLoggerFactory.get(GetRangesExecutors.class);
    private static final int GET_RANGES_QUEUE_SIZE_WARNING_THRESHOLD = 1000;

    private GetRangesExecutors() {
        // boo
    }

    @SuppressWarnings("DangerousThreadPoolExecutorUsage")
    public static ExecutorService createGetRangesExecutor(
            int numThreads, String prefix, Optional<Integer> sharedExecutorThreads) {
        String name = prefix + "-get-ranges";
        return wrap(SharedFixedExecutors.getOrCreateMaybeShared(name, numThreads, sharedExecutorThreads));
    }

    private static ExecutorService wrap(ExecutorService executor) {
        return new AbstractExecutorService() {
            private final RateLimitedLogger warningLogger = new RateLimitedLogger(log, 1);
            private final AtomicInteger queueSizeEstimate = new AtomicInteger();

            @Override
            public void shutdown() {
                executor.shutdown();
            }

            @Override
            public List<Runnable> shutdownNow() {
                return executor.shutdownNow();
            }

            @Override
            public boolean isShutdown() {
                return executor.isShutdown();
            }

            @Override
            public boolean isTerminated() {
                return executor.isTerminated();
            }

            @Override
            public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
                return executor.awaitTermination(timeout, unit);
            }

            @Override
            public void execute(Runnable command) {
                sanityCheckAndIncrementQueueSize();
                executor.execute(() -> {
                    queueSizeEstimate.getAndDecrement();
                    command.run();
                });
            }

            private void sanityCheckAndIncrementQueueSize() {
                int currentSize = queueSizeEstimate.getAndIncrement();
                if (currentSize >= GET_RANGES_QUEUE_SIZE_WARNING_THRESHOLD) {
                    warningLogger.log(logger -> logger.warn(
                            "You have {} pending getRanges tasks. Please sanity check both your level "
                                    + "of concurrency and size of batched range requests. If necessary you can "
                                    + "increase the value of concurrentGetRangesThreadPoolSize to allow for a larger "
                                    + "thread pool.",
                            SafeArg.of("currentSize", currentSize)));
                }
            }
        };
    }
}
