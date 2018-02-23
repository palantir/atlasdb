/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.transaction.impl;


import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.palantir.atlasdb.cache.TimestampCache;
import com.palantir.atlasdb.transaction.api.ConditionAwareTransactionTask;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionTask;
import com.palantir.atlasdb.transaction.api.PreCommitCondition;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionFailedException;
import com.palantir.atlasdb.transaction.api.TransactionFailedRetriableException;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.common.base.Throwables;
import com.palantir.exception.NotInitializedException;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockRequest;
import com.palantir.logsafe.SafeArg;

public abstract class AbstractTransactionManager implements TransactionManager {
    private static final int GET_RANGES_QUEUE_SIZE_WARNING_THRESHOLD = 1000;

    public static final Logger log = LoggerFactory.getLogger(AbstractTransactionManager.class);
    final TimestampCache timestampValidationReadCache;
    private volatile boolean closed = false;

    AbstractTransactionManager(Supplier<Long> timestampCacheSize) {
        this.timestampValidationReadCache = new TimestampCache(timestampCacheSize);
    }

    protected static final PreCommitCondition NO_OP_CONDITION = new PreCommitCondition() {
        @Override
        public void throwIfConditionInvalid(long timestamp) {}

        @Override
        public void cleanup() {}
    };

    protected static void sleepForBackoff(@SuppressWarnings("unused") int numTimesFailed) {
        // no-op
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
    public <T, C extends PreCommitCondition, E extends Exception> T runTaskWithConditionWithRetry(
            Supplier<C> conditionSupplier, ConditionAwareTransactionTask<T, C, E> task) throws E {
        int failureCount = 0;
        UUID runId = UUID.randomUUID();
        while (true) {
            checkOpen();
            try {
                C condition = conditionSupplier.get();
                T result = runTaskWithConditionThrowOnConflict(condition, task);
                if (failureCount > 0) {
                    log.info("[{}] Successfully completed transaction after {} retries.",
                            SafeArg.of("runId", runId),
                            SafeArg.of("failureCount", failureCount));
                }
                return result;
            } catch (TransactionFailedException e) {
                if (!e.canTransactionBeRetried()) {
                    log.warn("[{}] Non-retriable exception while processing transaction.",
                            SafeArg.of("runId", runId),
                            SafeArg.of("failureCount", failureCount));
                    throw e;
                }
                failureCount++;
                if (shouldStopRetrying(failureCount)) {
                    log.warn("[{}] Failing after {} tries.",
                            SafeArg.of("runId", runId),
                            SafeArg.of("failureCount", failureCount), e);
                    throw Throwables.rewrap(String.format("Failing after %d tries.", failureCount), e);
                }
                log.info("[{}] Retrying transaction after {} failure(s).",
                        SafeArg.of("runId", runId),
                        SafeArg.of("failureCount", failureCount), e);
            } catch (NotInitializedException e) {
                log.info("TransactionManager is not initialized. Aborting transaction with runTaskWithRetry", e);
                throw e;
            } catch (RuntimeException e) {
                log.warn("[{}] RuntimeException while processing transaction.", SafeArg.of("runId", runId), e);
                throw e;
            }
            sleepForBackoff(failureCount);
        }
    }

    @Override
    public <T, E extends Exception> T runTaskThrowOnConflict(TransactionTask<T, E> task) throws E {
        return runTaskWithConditionThrowOnConflict(NO_OP_CONDITION, (txn, condition) -> task.execute(txn));
    }

    @Override
    public <T, E extends Exception> T runTaskWithRetry(TransactionTask<T, E> task) throws E {
        return runTaskWithConditionWithRetry(() -> NO_OP_CONDITION, (txn, condition) -> task.execute(txn));
    }

    @Override
    public <T, E extends Exception> T runTaskReadOnly(TransactionTask<T, E> task) throws E {
        return runTaskWithConditionReadOnly(NO_OP_CONDITION, (transaction, condition) -> task.execute(transaction));
    }

    @Override
    public <T, E extends Exception> T runTaskWithLocksWithRetry(
            Iterable<HeldLocksToken> lockTokens,
            Supplier<LockRequest> lockSupplier,
            LockAwareTransactionTask<T, E> task) throws E, InterruptedException {
        checkOpen();
        Supplier<AdvisoryLocksCondition> conditionSupplier =
                AdvisoryLockConditionSuppliers.get(getLockService(), lockTokens, lockSupplier);
        return runTaskWithConditionWithRetry(conditionSupplier, (transaction, condition) ->
                task.execute(transaction, condition.getLocks()));
    }

    @Override
    public <T, E extends Exception> T runTaskWithLocksWithRetry(
            Supplier<LockRequest> lockSupplier,
            LockAwareTransactionTask<T, E> task)
            throws E, InterruptedException {
        checkOpen();
        return runTaskWithLocksWithRetry(ImmutableList.of(), lockSupplier, task);
    }

    @Override
    public <T, E extends Exception> T runTaskWithLocksThrowOnConflict(
            Iterable<HeldLocksToken> lockTokens,
            LockAwareTransactionTask<T, E> task)
            throws E, TransactionFailedRetriableException {
        checkOpen();
        AdvisoryLocksCondition lockCondition =
                new ExternalLocksCondition(getLockService(), ImmutableSet.copyOf(lockTokens));
        return runTaskWithConditionThrowOnConflict(lockCondition,
                (transaction, condition) -> task.execute(transaction, condition.getLocks()));
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
        BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<Runnable>() {
            private final RateLimiter warningRateLimiter = RateLimiter.create(1);

            @Override
            public boolean offer(Runnable runnable) {
                sanityCheckQueueSize();
                return super.offer(runnable);
            }

            private void sanityCheckQueueSize() {
                int currentSize = this.size();
                if (currentSize >= GET_RANGES_QUEUE_SIZE_WARNING_THRESHOLD && warningRateLimiter.tryAcquire()) {
                    log.warn("You have {} pending getRanges tasks. Please sanity check both your level "
                            + "of concurrency and size of batched range requests. If necessary you can "
                            + "increase the value of concurrentGetRangesThreadPoolSize to allow for a larger "
                            + "thread pool.", currentSize);
                }
            }
        };
        return new ThreadPoolExecutor(
                numThreads, numThreads, 0L, TimeUnit.MILLISECONDS, workQueue,
                new ThreadFactoryBuilder().setNameFormat(
                        AbstractTransactionManager.this.getClass().getSimpleName() + "-get-ranges-%d").build());
    }
}
