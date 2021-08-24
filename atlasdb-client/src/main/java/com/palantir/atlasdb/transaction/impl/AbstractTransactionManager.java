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
package com.palantir.atlasdb.transaction.impl;

import com.google.common.util.concurrent.RateLimiter;
import com.palantir.atlasdb.cache.TimestampCache;
import com.palantir.atlasdb.health.MetricsBasedTimelockHealthCheck;
import com.palantir.atlasdb.health.TimelockHealthCheck;
import com.palantir.atlasdb.transaction.api.TimelockServiceStatus;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionFailedException;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class AbstractTransactionManager implements TransactionManager {
    private static final SafeLogger log = SafeLoggerFactory.get(AbstractTransactionManager.class);
    private static final int GET_RANGES_QUEUE_SIZE_WARNING_THRESHOLD = 1000;

    final TimestampCache timestampValidationReadCache;
    private volatile boolean closed = false;

    private final TimelockHealthCheck timelockHealthCheck;

    AbstractTransactionManager(MetricsManager metricsManager, TimestampCache timestampCache) {
        this.timelockHealthCheck = new MetricsBasedTimelockHealthCheck(metricsManager);
        this.timestampValidationReadCache = timestampCache;
    }

    protected boolean shouldStopRetrying(@SuppressWarnings("unused") int numTimesFailed) {
        return false;
    }

    protected final <T, E extends Exception> T runTaskThrowOnConflict(TransactionTask<T, E> task, Transaction txn)
            throws E, TransactionFailedException {
        checkOpen();
        try {
            T ret = task.execute(txn);
            if (txn.isUncommitted()) {
                txn.commit();
            }
            return ret;
        } finally {
            // Make sure that anyone trying to retain a reference to this transaction
            // will not be able to use it.
            if (txn.isUncommitted()) {
                txn.abort();
            }
        }
    }

    @Override
    public void close() {
        this.closed = true;
    }

    /**
     * Checks that the transaction manager is open.
     *
     * @throws IllegalStateException if the transaction manager has been closed.
     */
    protected void checkOpen() {
        Preconditions.checkState(!this.closed, "Operations cannot be performed on closed TransactionManager.");
    }

    @Override
    public void clearTimestampCache() {
        timestampValidationReadCache.clear();
    }

    @SuppressWarnings("DangerousThreadPoolExecutorUsage")
    ExecutorService createGetRangesExecutor(int numThreads) {
        ExecutorService executor = PTExecutors.newFixedThreadPool(
                numThreads, AbstractTransactionManager.this.getClass().getSimpleName() + "-get-ranges");

        return new AbstractExecutorService() {
            private final AtomicInteger queueSizeEstimate = new AtomicInteger();
            private final RateLimiter warningRateLimiter = RateLimiter.create(1);

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
                if (currentSize >= GET_RANGES_QUEUE_SIZE_WARNING_THRESHOLD && warningRateLimiter.tryAcquire()) {
                    log.warn(
                            "You have {} pending getRanges tasks. Please sanity check both your level "
                                    + "of concurrency and size of batched range requests. If necessary you can "
                                    + "increase the value of concurrentGetRangesThreadPoolSize to allow for a larger "
                                    + "thread pool.",
                            SafeArg.of("currentSize", currentSize));
                }
            }
        };
    }

    @Override
    public TimelockServiceStatus getTimelockServiceStatus() {
        return timelockHealthCheck.getStatus();
    }
}
