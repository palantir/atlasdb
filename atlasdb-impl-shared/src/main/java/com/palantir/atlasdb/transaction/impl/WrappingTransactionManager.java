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

import com.google.common.base.Supplier;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.transaction.api.AutoDelegate_TransactionManager;
import com.palantir.atlasdb.transaction.api.ConditionAwareTransactionTask;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionTask;
import com.palantir.atlasdb.transaction.api.PreCommitCondition;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionConflictException;
import com.palantir.atlasdb.transaction.api.TransactionFailedRetriableException;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockService;
import com.palantir.lock.v2.TimelockService;
import com.palantir.processors.AutoDelegate;
import com.palantir.timestamp.TimestampService;

@AutoDelegate(typeToExtend = TransactionManager.class)
public abstract class WrappingTransactionManager implements AutoDelegate_TransactionManager {
    private final TransactionManager delegate;

    public WrappingTransactionManager(TransactionManager delegate) {
        this.delegate = delegate;
    }

    @Override
    public TransactionManager delegate() {
        return delegate;
    }

    protected abstract Transaction wrap(Transaction transaction);

    @Override
    public <T, C extends PreCommitCondition, E extends Exception> T runTaskWithConditionThrowOnConflict(
            C condition,
            ConditionAwareTransactionTask<T, C, E> task)
            throws E, TransactionFailedRetriableException {
        return delegate().runTaskWithConditionThrowOnConflict(condition, wrapTask(task));
    }

    public <T, C extends PreCommitCondition, E extends Exception> T runTaskWithConditionReadOnly(
            C condition,
            ConditionAwareTransactionTask<T, C, E> task)
            throws E {
        return delegate().runTaskWithConditionReadOnly(condition, wrapTask(task));
    }

    @Override
    public <T, C extends PreCommitCondition, E extends Exception> T runTaskWithConditionWithRetry(
            Supplier<C> conditionSupplier,
            ConditionAwareTransactionTask<T, C, E> task)
            throws E {
        return delegate().runTaskWithConditionWithRetry(conditionSupplier, wrapTask(task));
    }

    @Override
    public KeyValueService getKeyValueService() {
        return delegate.getKeyValueService();
    }

    @Override
    public LockService getLockService() {
        return delegate.getLockService();
    }

    @Override
    public TimelockService getTimelockService() {
        return delegate.getTimelockService();
    }

    @Override
    public TimestampService getTimestampService() {
        return delegate.getTimestampService();
    }

    private <T, C extends PreCommitCondition, E extends Exception> ConditionAwareTransactionTask<T, C, E> wrapTask(
            ConditionAwareTransactionTask<T, C, E> task) {
        return (transaction, condition) -> task.execute(wrap(transaction), condition);
    }
}
