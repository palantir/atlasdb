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

import com.google.common.base.Supplier;
import com.palantir.atlasdb.transaction.api.ConditionAwareTransactionTask;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionTask;
import com.palantir.atlasdb.transaction.api.PreCommitCondition;
import com.palantir.atlasdb.transaction.api.TransactionFailedRetriableException;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.atlasdb.transaction.api.TransactionTaskWrapper;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockRequest;

public class WrappingSerializableTransactionManager extends AutoDelegate_SerializableTransactionManager {
    private final SerializableTransactionManager delegate;
    private final TransactionTaskWrapper wrapper;

    public WrappingSerializableTransactionManager(SerializableTransactionManager delegate,
            TransactionTaskWrapper wrapper) {
        this.delegate = delegate;
        this.wrapper = wrapper;
    }

    @Override
    public SerializableTransactionManager delegate() {
        return delegate;
    }

    @Override
    public <T, E extends Exception> T finishRunTaskWithLockThrowOnConflict(RawTransaction tx,
            TransactionTask<T, E> task) throws E, TransactionFailedRetriableException {
        return super.finishRunTaskWithLockThrowOnConflict(tx, wrapper.wrap(task));
    }

    @Override
    public <T, C extends PreCommitCondition, E extends Exception> T runTaskWithConditionThrowOnConflict(C condition,
            ConditionAwareTransactionTask<T, C, E> task) throws E, TransactionFailedRetriableException {
        return super.runTaskWithConditionThrowOnConflict(condition, wrapper.wrap(task));
    }

    @Override
    public <T, E extends Exception> T runTaskReadOnly(TransactionTask<T, E> task) throws E {
        return super.runTaskReadOnly(wrapper.wrap(task));
    }

    @Override
    public <T, C extends PreCommitCondition, E extends Exception> T runTaskReadOnlyWithCondition(C condition,
            ConditionAwareTransactionTask<T, C, E> task) throws E {
        return super.runTaskReadOnlyWithCondition(condition, wrapper.wrap(task));
    }

    @Override
    public <T, E extends Exception> T runTaskWithLocksWithRetry(Iterable<HeldLocksToken> lockTokens,
            Supplier<LockRequest> lockSupplier, LockAwareTransactionTask<T, E> task) throws E, InterruptedException {
        return super.runTaskWithLocksWithRetry(lockTokens, lockSupplier, wrapper.wrap(task));
    }

    @Override
    public <T, E extends Exception> T runTaskWithLocksWithRetry(Supplier<LockRequest> lockSupplier,
            LockAwareTransactionTask<T, E> task) throws E, InterruptedException {
        return super.runTaskWithLocksWithRetry(lockSupplier, wrapper.wrap(task));
    }

    @Override
    public <T, E extends Exception> T runTaskWithLocksThrowOnConflict(Iterable<HeldLocksToken> lockTokens,
            LockAwareTransactionTask<T, E> task) throws E, TransactionFailedRetriableException {
        return super.runTaskWithLocksThrowOnConflict(lockTokens, wrapper.wrap(task));
    }

    @Override
    public <T, C extends PreCommitCondition, E extends Exception> T runTaskWithConditionWithRetry(
            Supplier<C> conditionSupplier, ConditionAwareTransactionTask<T, C, E> task) throws E {
        return super.runTaskWithConditionWithRetry(conditionSupplier, wrapper.wrap(task));
    }

    @Override
    public <T, E extends Exception> T runTaskThrowOnConflict(TransactionTask<T, E> task) throws E {
        return super.runTaskThrowOnConflict(wrapper.wrap(task));
    }

    @Override
    public <T, E extends Exception> T runTaskWithRetry(TransactionTask<T, E> task) throws E {
        return super.runTaskWithRetry(wrapper.wrap(task));
    }
}
