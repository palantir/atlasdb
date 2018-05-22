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
package com.palantir.atlasdb.transaction.api;

import com.google.common.base.Supplier;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockService;
import com.palantir.lock.v2.TimelockService;

public interface LockAwareTransactionManager extends TransactionManager {
    /**
     * This method is basically the same as {@link #runTaskWithRetry(TransactionTask)} but it will
     * acquire locks right before the transaction is created and release them after the task is complete.
     * <p>
     * The created transaction will not commit successfully if these locks are invalid by the time commit is run.
     *
     * @param lockSupplier supplier for the lock request
     * @param task task to run
     *
     * @return value returned by task
     *
     * @throws LockAcquisitionException If the supplied lock request is not successfully acquired.
     * @throws IllegalStateException if the transaction manager has been closed.
     */
    <T, E extends Exception> T runTaskWithLocksWithRetry(
            Supplier<LockRequest> lockSupplier,
            LockAwareTransactionTask<T, E> task) throws E, InterruptedException, LockAcquisitionException;

    /**
     * This method is the same as {@link #runTaskWithLocksWithRetry(Supplier, LockAwareTransactionTask)}
     * but it will also ensure that the existing lock tokens passed are still valid before committing.
     *
     * @param lockTokens lock tokens to acquire while transaction executes
     * @param task task to run
     *
     * @return value returned by task
     *
     * @throws LockAcquisitionException If the supplied lock request is not successfully acquired.
     * @throws IllegalStateException if the transaction manager has been closed.
     */
    <T, E extends Exception> T runTaskWithLocksWithRetry(
            Iterable<HeldLocksToken> lockTokens,
            Supplier<LockRequest> lockSupplier,
            LockAwareTransactionTask<T, E> task) throws E, InterruptedException, LockAcquisitionException;

    /**
     * This method is the same as {@link #runTaskThrowOnConflict(TransactionTask)} except the created transaction
     * will not commit successfully if these locks are invalid by the time commit is run.
     *
     * @param lockTokens lock tokens to refresh while transaction executes
     * @param task task to run
     *
     * @return value returned by task
     *
     * @throws IllegalStateException if the transaction manager has been closed.
     */
    <T, E extends Exception> T runTaskWithLocksThrowOnConflict(
            Iterable<HeldLocksToken> lockTokens,
            LockAwareTransactionTask<T, E> task) throws E, TransactionFailedRetriableException;

    /**
     * Returns the lock service used by this transaction manager.
     *
     * @return the lock service for this transaction manager
     */
    LockService getLockService();

    TimelockService getTimelockService();
}
