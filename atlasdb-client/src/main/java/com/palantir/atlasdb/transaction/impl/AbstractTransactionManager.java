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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.cache.TimestampCache;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionFailedException;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.common.base.Throwables;

public abstract class AbstractTransactionManager implements TransactionManager {
    public static final Logger log = LoggerFactory.getLogger(AbstractTransactionManager.class);
    protected final TimestampCache timestampValidationReadCache = TimestampCache.create();
    private volatile boolean closed = false;

    @Override
    public <T, E extends Exception> T runTaskWithRetry(TransactionTask<T, E> task) throws E {
        int failureCount = 0;
        UUID runId = UUID.randomUUID();
        while (true) {
            checkOpen();
            try {
                T result = runTaskThrowOnConflict(task);
                if (failureCount > 0) {
                    log.info("[{}] Successfully completed transaction after {} retries.", runId, failureCount);
                }
                return result;
            } catch (TransactionFailedException e) {
                if (!e.canTransactionBeRetried()) {
                    log.warn("[{}] Non-retriable exception while processing transaction.", runId, e);
                    throw e;
                }
                failureCount++;
                if (shouldStopRetrying(failureCount)) {
                    log.warn("[{}] Failing after {} tries.", runId, failureCount, e);
                    throw Throwables.rewrap(String.format("Failing after %d tries.", failureCount), e);
                }
                log.info("[{}] Retrying transaction after {} failure(s).", runId, failureCount, e);
            } catch (RuntimeException e) {
                log.warn("[{}] RuntimeException while processing transaction.", runId, e);
                throw e;
            }
            sleepForBackoff(failureCount);
        }
    }

    protected void sleepForBackoff(@SuppressWarnings("unused") int numTimesFailed) {
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
}
