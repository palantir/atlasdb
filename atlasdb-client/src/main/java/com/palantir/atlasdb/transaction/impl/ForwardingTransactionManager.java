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

import com.google.common.collect.ForwardingObject;
import com.palantir.atlasdb.transaction.api.TransactionFailedRetriableException;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.api.TransactionTask;

public abstract class ForwardingTransactionManager extends ForwardingObject implements TransactionManager {
    @Override
    protected abstract TransactionManager delegate();

    @Override
    public boolean isInitialized() {
        return delegate().isInitialized();
    }

    @Override
    public <T, E extends Exception> T runTaskWithRetry(TransactionTask<T, E> task)
            throws E {
        return delegate().runTaskWithRetry(task);
    }

    @Override
    public <T, E extends Exception> T runTaskThrowOnConflict(TransactionTask<T, E> task)
            throws E, TransactionFailedRetriableException {
        return delegate().runTaskThrowOnConflict(task);
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
    public <T, E extends Exception> T runTaskReadOnly(TransactionTask<T, E> task)
            throws E {
        return delegate().runTaskReadOnly(task);
    }

    @Override
    public void clearTimestampCache() {
        delegate().clearTimestampCache();
    }

    @Override
    public void close() {
        delegate().close();
    }
}
