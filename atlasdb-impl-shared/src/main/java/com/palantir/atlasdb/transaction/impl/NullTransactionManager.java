/**
 * // Copyright 2015 Palantir Technologies
 * //
 * // Licensed under the BSD-3 License (the "License");
 * // you may not use this file except in compliance with the License.
 * // You may obtain a copy of the License at
 * //
 * // http://opensource.org/licenses/BSD-3-Clause
 * //
 * // Unless required by applicable law or agreed to in writing, software
 * // distributed under the License is distributed on an "AS IS" BASIS,
 * // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * // See the License for the specific language governing permissions and
 * // limitations under the License.
 */
package com.palantir.atlasdb.transaction.impl;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionTask;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionTasks;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionFailedRetriableException;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockService;
import com.palantir.timestamp.TimestampService;

public class NullTransactionManager extends AbstractLockAwareTransactionManager {
    final KeyValueService service;
    final TimestampService timeService;
    final AtlasDbConstraintCheckingMode constraintCheckingMode;

    public NullTransactionManager(KeyValueService service, TimestampService time, AtlasDbConstraintCheckingMode constraintCheckingMode) {
        this.service = service;
        this.timeService = time;
        this.constraintCheckingMode = constraintCheckingMode;
    }

    @Override
    public <T, E extends Exception> T runTaskReadOnly(TransactionTask<T, E> task) throws E {
        Transaction t = new NullTransaction(service, timeService.getFreshTimestamp(), false);
        return runTaskThrowOnConflict(task, new UnmodifiableTransaction(t) {
            @Override public void abort() {/* do nothing */}
            @Override public void commit() {/* do nothing */}
        });
    }

    @Override
    public long getImmutableTimestamp() {
        return timeService.getFreshTimestamp();
    }

    @Override
    public long getUnreadableTimestamp() {
        return timeService.getFreshTimestamp();
    }

    @Override
    public LockService getLockService() {
        return null;
    }

    @Override
    public <T, E extends Exception> T runTaskWithLocksThrowOnConflict(Iterable<HeldLocksToken> lockTokens,
                                                                      LockAwareTransactionTask<T, E> task)
                                                                              throws E, TransactionFailedRetriableException {
        Transaction t = new NullTransaction(service, timeService.getFreshTimestamp(), false);
        return runTaskThrowOnConflict(LockAwareTransactionTasks.asLockUnaware(task), t);
    }
}
