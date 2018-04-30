/*
 * (c) Copyright 2015 Palantir Technologies Inc. All rights reserved.
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
import com.palantir.atlasdb.transaction.api.KeyValueServiceStatus;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionManager;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionTask;
import com.palantir.atlasdb.transaction.api.TimelockServiceStatus;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionConflictException;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockService;
import com.palantir.lock.v2.TimelockService;

public abstract class WrappingTransactionManager extends ForwardingLockAwareTransactionManager {
    private final LockAwareTransactionManager delegate;

    public WrappingTransactionManager(LockAwareTransactionManager delegate) {
        this.delegate = delegate;
    }

    @Override
    protected LockAwareTransactionManager delegate() {
        return delegate;
    }

    protected abstract Transaction wrap(Transaction transaction);

    @Override
    public <T, E extends Exception> T runTaskWithRetry(TransactionTask<T, E> task) throws E {
        return delegate().runTaskWithRetry(wrapTask(task));
    }

    @Override
    public <T, E extends Exception> T runTaskThrowOnConflict(TransactionTask<T, E> task) throws E,
            TransactionConflictException {
        return delegate().runTaskThrowOnConflict(wrapTask(task));
    }

    @Override
    public long getImmutableTimestamp() {
        return delegate().getImmutableTimestamp();
    }

    @Override
    public KeyValueServiceStatus getKeyValueServiceStatus() {
        return delegate().getKeyValueServiceStatus();
    }

    @Override
    public TimelockServiceStatus getTimelockServiceStatus() {
        return delegate().getTimelockServiceStatus();
    }

    @Override
    public long getUnreadableTimestamp() {
        return delegate().getUnreadableTimestamp();
    }

    @Override
    public LockService getLockService() {
        return delegate.getLockService();
    }

    @Override
    public TimelockService getTimelockService() {
        return delegate.getTimelockService();
    }

    private <T, E extends Exception> TransactionTask<T, E> wrapTask(TransactionTask<T, E> task) {
        return transaction -> task.execute(wrap(transaction));
    }

    private <T, E extends Exception> LockAwareTransactionTask<T, E> wrapTask(LockAwareTransactionTask<T, E> task) {
        return (transaction, locks) -> task.execute(wrap(transaction), locks);
    }

    @Override
    public <T, E extends Exception> T runTaskWithLocksThrowOnConflict(
            Iterable<HeldLocksToken> lockTokens,
            LockAwareTransactionTask<T, E> task)
            throws E, TransactionConflictException {
        return delegate().runTaskWithLocksThrowOnConflict(lockTokens, wrapTask(task));
    }

    @Override
    public <T, E extends Exception> T runTaskWithLocksWithRetry(
            Supplier<LockRequest> lockSupplier,
            LockAwareTransactionTask<T, E> task)
            throws E, InterruptedException {
        return delegate().runTaskWithLocksWithRetry(lockSupplier, wrapTask(task));
    }

    @Override
    public <T, E extends Exception> T runTaskWithLocksWithRetry(
            Iterable<HeldLocksToken> lockTokens,
            Supplier<LockRequest> lockSupplier,
            LockAwareTransactionTask<T, E> task)
            throws E, InterruptedException {
        return delegate().runTaskWithLocksWithRetry(lockTokens, lockSupplier, wrapTask(task));
    }

    @Override
    public <T, E extends Exception> T runTaskReadOnly(TransactionTask<T, E> task) throws E {
        return delegate().runTaskReadOnly(wrapTask(task));
    }

    @Override
    public void close() {
        delegate().close();
    }
}
