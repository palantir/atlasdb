/**
 * Copyright 2015 Palantir Technologies
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


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionFailedException;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.common.base.Throwables;

public abstract class AbstractTransactionManager implements TransactionManager {
    public static final Logger log = LoggerFactory.getLogger(AbstractTransactionManager.class);

    protected boolean isClosed;

    @Override
    public <T, E extends Exception> T runTaskWithRetry(TransactionTask<T, E> task) throws E {
        Preconditions.checkState(checkOpen(), "Operations cannot be performed on closed TransactionManager.");
        int failureCount = 0;
        while (true) {
            try {
                return runTaskThrowOnConflict(task);
            } catch (TransactionFailedException e) {
                if (!e.canTransactionBeRetried()) {
                    log.warn("Non-retriable exception while processing transaction.", e);
                    throw e;
                }
                failureCount++;
                if (shouldStopRetrying(failureCount)) {
                    String msg = "Failing after " + failureCount + " tries";
                    log.warn(msg, e);
                    throw Throwables.rewrap(msg, e);
                }
                log.info("retrying transaction", e);
            } catch (RuntimeException e) {
                log.warn("RuntimeException while processing transaction.", e);
                throw e;
            }
            sleepForBackoff(failureCount);
        }
    }

    protected void sleepForBackoff(@SuppressWarnings("unused") int numTimesFailed) {
        Preconditions.checkState(checkOpen(), "Operations cannot be performed on closed TransactionManager.");
        // no-op
    }

    protected boolean shouldStopRetrying(@SuppressWarnings("unused") int numTimesFailed) {
        Preconditions.checkState(checkOpen(), "Operations cannot be performed on closed TransactionManager.");
        return false;
    }

    final protected <T, E extends Exception> T runTaskThrowOnConflict(TransactionTask<T, E> task, Transaction t)
            throws E, TransactionFailedException {
        Preconditions.checkState(checkOpen(), "Operations cannot be performed on closed TransactionManager.");
        try {
            T ret = task.execute(t);
            if (t.isUncommitted()) {
                t.commit();
            }
            return ret;
        } finally {
            // Make sure that anyone trying to retain a reference to this transaction
            // will not be able to use it.
            if (t.isUncommitted()) {
                t.abort();
            }
        }
    }

    @Override
    public void close() {
        if (checkOpen()) {
            isClosed = true;
        }
    }

    protected boolean checkOpen() {
        return !isClosed;
    }
}
