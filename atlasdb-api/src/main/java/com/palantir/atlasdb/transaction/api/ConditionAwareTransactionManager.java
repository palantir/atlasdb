/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

public interface ConditionAwareTransactionManager extends TransactionManager {

    /**
     * This method is basically the same as {@link #runTaskWithRetry(TransactionTask)}, but it will
     * acquire a {@link PreCommitCondition} right before the transaction is created and check it
     * immediately before the transaction commits.
     * <p>
     * The created transaction will not commit successfully if the check fails.
     *
     * @param conditionSupplier supplier for the condition
     * @param task task to run
     *
     * @return value returned by task
     *
     * @throws IllegalStateException if the transaction manager has been closed.
     */
    <T, C extends PreCommitCondition, E extends Exception> T runTaskWithConditionWithRetry(
            Supplier<PreCommitCondition> conditionSupplier,
            ConditionAwareTransactionTask<T, C, E> task) throws E, InterruptedException;

    /**
     * This method is basically the same as {@link #runTaskThrowOnConflict(TransactionTask)}, but it will
     * acquire a {@link PreCommitCondition} right before the transaction is created and check it
     * immediately before the transaction commits.
     * <p>
     * The created transaction will not commit successfully if the check fails.
     *
     * @param conditionSupplier supplier for the condition
     * @param task task to run
     *
     * @return value returned by task
     *
     * @throws IllegalStateException if the transaction manager has been closed.
     */
    <T, C extends PreCommitCondition, E extends Exception> T runTaskWithConditionThrowOnConflict(
            Supplier<PreCommitCondition> conditionSupplier,
            ConditionAwareTransactionTask<T, C, E> task)
            throws E, InterruptedException, TransactionFailedRetriableException;

    /**
     * This method is basically the same as {@link #runTaskReadOnly(TransactionTask)}, but it will
     * acquire a {@link PreCommitCondition} right before the transaction is created and check it
     * immediately after fetching a read timestamp.
     * <p>
     * The created transaction will fail if the check is no longer valid after fetching the read
     * timestamp.
     *
     * @param conditionSupplier supplier for the condition
     * @param task task to run
     *
     * @return value returned by task
     *
     * @throws IllegalStateException if the transaction manager has been closed.
     */
    <T, C extends PreCommitCondition, E extends Exception> T runTaskReadOnlyWithCondition(
            Supplier<PreCommitCondition> conditionSupplier,
            ConditionAwareTransactionTask<T, C, E> task) throws E;
}
