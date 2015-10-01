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

import com.google.common.base.Supplier;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionManager;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionTask;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionConflictException;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.RemoteLockService;

public abstract class WrappingTransactionManager extends ForwardingLockAwareTransactionManager {

    private final LockAwareTransactionManager delegate;

    public WrappingTransactionManager(LockAwareTransactionManager delegate) {
        this.delegate = delegate;
    }

    @Override
    protected LockAwareTransactionManager delegate() {
        return delegate;
    }

    protected abstract Transaction wrap(Transaction t);

    @Override
    public <T, E extends Exception> T runTaskWithRetry(final TransactionTask<T, E> task) throws E {
        return delegate().runTaskWithRetry(wrapTask(task));
    }

    @Override
    public <T, E extends Exception> T runTaskThrowOnConflict(final TransactionTask<T, E> task) throws E,
            TransactionConflictException {
        return delegate().runTaskThrowOnConflict(wrapTask(task));
    }

    @Override
    public long getImmutableTimestamp() {
        return delegate().getImmutableTimestamp();
    }

    @Override
    public long getUnreadableTimestamp() {
        return delegate().getUnreadableTimestamp();
    }

    @Override
    public RemoteLockService getLockService() {
        return delegate.getLockService();
    }

    private <T, E extends Exception> TransactionTask<T, E> wrapTask(final TransactionTask<T, E> task) {
        return new TransactionTask<T, E>() {
            @Override
            public T execute(Transaction t) throws E {
                return task.execute(wrap(t));
            }
        };
    }

    private <T, E extends Exception> LockAwareTransactionTask<T, E> wrapTask(final LockAwareTransactionTask<T, E> task) {
        return new LockAwareTransactionTask<T, E>() {
            @Override
            public T execute(Transaction t, Iterable<LockRefreshToken> locks) throws E {
                return task.execute(wrap(t), locks);
            }
        };
    }

    @Override
    public <T, E extends Exception> T runTaskWithLocksThrowOnConflict(Iterable<LockRefreshToken> lockTokens,
                                                                      LockAwareTransactionTask<T, E> task) throws E,
            TransactionConflictException {
        return delegate().runTaskWithLocksThrowOnConflict(lockTokens, wrapTask(task));
    }

    @Override
    public <T, E extends Exception> T runTaskWithLocksWithRetry(Supplier<LockRequest> lockSupplier,
                                                                LockAwareTransactionTask<T, E> task) throws E,
            InterruptedException {
        return delegate().runTaskWithLocksWithRetry(lockSupplier, wrapTask(task));
    }

    @Override
    public <T, E extends Exception> T runTaskReadOnly(TransactionTask<T, E> task) throws E {
        return delegate().runTaskReadOnly(wrapTask(task));
    }

    @Override
    public <T, E extends Exception> T runTaskWithLocksWithRetry(Iterable<LockRefreshToken> lockTokens,
                                                                Supplier<LockRequest> lockSupplier,
                                                                LockAwareTransactionTask<T, E> task)
            throws E, InterruptedException {
        return delegate().runTaskWithLocksWithRetry(lockTokens, lockSupplier, wrapTask(task));
    }

}
