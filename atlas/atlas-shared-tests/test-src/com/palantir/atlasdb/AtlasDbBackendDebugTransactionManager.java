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

package com.palantir.atlasdb;

import com.google.common.base.Supplier;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionManager;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionTask;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionTasks;
import com.palantir.atlasdb.transaction.api.TransactionConflictException;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.atlasdb.transaction.impl.ForwardingLockAwareTransactionManager;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockRequest;

class AtlasDbBackendDebugTransactionManager extends ForwardingLockAwareTransactionManager {
    private final LockAwareTransactionManager delegate;

    public AtlasDbBackendDebugTransactionManager(LockAwareTransactionManager delegate) {
        this.delegate = delegate;
    }

    @Override
    protected LockAwareTransactionManager delegate() {
        return delegate;
    }

    @Override
    public <T, E extends Exception> T runTaskWithLocksWithRetry(Supplier<LockRequest> lockSupplier,
                                                                LockAwareTransactionTask<T, E> task)
            throws E {
        return runTaskReadOnly(LockAwareTransactionTasks.asLockUnaware(task));
    }

    @Override
    public <T, E extends Exception> T runTaskWithLocksWithRetry(Iterable<HeldLocksToken> lockTokens,
                                                                Supplier<LockRequest> lockSupplier,
                                                                LockAwareTransactionTask<T, E> task)
            throws E {
        return runTaskReadOnly(LockAwareTransactionTasks.asLockUnaware(task));
    }

    @Override
    public <T, E extends Exception> T runTaskWithLocksThrowOnConflict(Iterable<HeldLocksToken> lockTokens,
                                                                      LockAwareTransactionTask<T, E> task)
            throws E, TransactionConflictException {
        return runTaskReadOnly(LockAwareTransactionTasks.asLockUnaware(task));
    }

    @Override
    public <T, E extends Exception> T runTaskWithRetry(TransactionTask<T, E> task)
            throws E {
        return runTaskReadOnly(task);
    }

    @Override
    public <T, E extends Exception> T runTaskThrowOnConflict(TransactionTask<T, E> task)
            throws E, TransactionConflictException {
        return runTaskReadOnly(task);
    }
}
