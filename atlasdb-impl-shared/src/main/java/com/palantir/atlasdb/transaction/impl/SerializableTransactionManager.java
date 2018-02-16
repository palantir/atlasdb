/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

import com.palantir.atlasdb.cleaner.Cleaner;
import com.palantir.atlasdb.transaction.api.PreCommitCondition;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.api.TransactionTask;

public interface SerializableTransactionManager extends TransactionManager {
    /**
     * @return A {@link SerializableTransaction} that can be used for reads and writes.
     * This behaves as the transactions used in the {@link TransactionManager#runTaskWithRetry(TransactionTask)}.
     */
    <C extends PreCommitCondition, E extends Exception> ServiceWriteTransaction getWriteTransaction(
            C condition) throws E;

    /**
     * @return A transaction that can just be used for readonly transactions.
     * Note that this transaction does not lock the immutable timestamp.
     * Therefore, it is possible that sweep has deleted the values this transaction would read,
     * causing correctness issues. The burden for locking the immutable timestamp, and preventing correctness problems
     * is on the client.
     */
    <C extends PreCommitCondition, E extends Exception> ServiceReadOnlyTransaction getReadTransaction(
            C condition) throws E;

    /**
     * Registers a Runnable that will be run when the transaction manager is closed, provided no callback already
     * submitted throws an exception.
     *
     * Concurrency: If this method races with close(), then closingCallback may not be called.
     */
    void registerClosingCallback(Runnable closingCallback);

    Cleaner getCleaner();
}
