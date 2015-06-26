// Copyright 2015 Palantir Technologies
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.atlasdb.transaction.api;

import com.google.common.base.Supplier;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.RemoteLockService;

public interface LockAwareTransactionManager extends TransactionManager {
    /**
     * This method is basically the same as {@link #runTaskWithRetry(TransactionTask)} but it will
     * acquire locks right before the transaction is created and release them after the task is complete.
     * <p>
     * The created transaction will not commit successfully if these locks are invalid by the time commit is run.
     * <p>
     * @throws LockAcquisitionException If the supplied lock request is not successfully acquired.
     */
    <T, E extends Exception> T runTaskWithLocksWithRetry(Supplier<LockRequest> lockSupplier,
                                                         LockAwareTransactionTask<T, E> task) throws E, InterruptedException, LockAcquisitionException;

    /**
     * This method is the same as {@link #runTaskWithLocksWithRetry(Supplier, TransactionTask)}
     * but it will also ensure that the existing lock tokens passed are still valid before committing.
     * <p>
     * @throws LockAcquisitionException If the supplied lock request is not successfully acquired.
     */
    <T, E extends Exception> T runTaskWithLocksWithRetry(Iterable<HeldLocksToken> lockTokens,
                                                         Supplier<LockRequest> lockSupplier,
                                                         LockAwareTransactionTask<T, E> task) throws E, InterruptedException, LockAcquisitionException;

    /**
     * This method is the same as {@link #runTaskThrowOnConflict(TransactionTask)} except the created transaction
     * will not commit successfully if these locks are invalid by the time commit is run.
     */
    <T, E extends Exception> T runTaskWithLocksThrowOnConflict(Iterable<HeldLocksToken> lockTokens,
                                                               LockAwareTransactionTask<T, E> task) throws E, TransactionFailedRetriableException;


    RemoteLockService getLockService();

}
