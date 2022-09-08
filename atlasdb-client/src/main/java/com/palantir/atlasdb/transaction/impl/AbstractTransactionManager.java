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

import com.palantir.atlasdb.cache.CommitStateCache;
import com.palantir.atlasdb.health.MetricsBasedTimelockHealthCheck;
import com.palantir.atlasdb.health.TimelockHealthCheck;
import com.palantir.atlasdb.transaction.api.TimelockServiceStatus;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionFailedException;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.logsafe.Preconditions;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

public abstract class AbstractTransactionManager implements TransactionManager {
    final CommitStateCache timestampValidationReadCache;
    private volatile boolean closed = false;

    private final TimelockHealthCheck timelockHealthCheck;

    AbstractTransactionManager(MetricsManager metricsManager, CommitStateCache commitStateCache) {
        this.timelockHealthCheck = new MetricsBasedTimelockHealthCheck(metricsManager);
        this.timestampValidationReadCache = commitStateCache;
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

    protected final <T, E extends Exception> T runTaskThrowOnConflictWithCallback(
            TransactionTask<T, E> task, CallbackAwareTransaction txn, Runnable callback)
            throws E, TransactionFailedException {
        try {
            checkOpen();
            try {
                T ret = task.execute(txn);
                if (txn.isUncommitted()) {
                    txn.commitWithoutCallbacks();
                }
                return ret;
            } finally {
                // Make sure that anyone trying to retain a reference to this transaction
                // will not be able to use it.
                if (txn.isUncommitted()) {
                    txn.abort();
                }
            }
        } finally {
            callback.run();
            txn.runSuccessCallbacksIfDefinitivelyCommitted();
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

    ExecutorService createGetRangesExecutor(int numThreads, Optional<Integer> sharedExecutorSize) {
        return GetRangesExecutors.createGetRangesExecutor(
                numThreads, AbstractTransactionManager.this.getClass().getSimpleName(), sharedExecutorSize);
    }

    @Override
    public TimelockServiceStatus getTimelockServiceStatus() {
        return timelockHealthCheck.getStatus();
    }
}
