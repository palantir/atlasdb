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

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionFailedNonRetriableException;
import com.palantir.atlasdb.transaction.api.TransactionLockTimeoutException;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;

/**
 *  Best effort attempt to keep backwards compatibility while making immutableTs lock validation optional on reads.
 *  Validating immutableTs lock only on commits rather than on every read may cause the TransactionTask to throw
 *  an unexpected non-retriable error as reads are not idempotent. This wrapper task will convert the exception thrown
 *  by the underlying task into a retriable lock timeout exception if immutableTs lock is not valid anymore.
 */
public class LockCheckingTransactionTask<T, E extends Exception> implements TransactionTask<T, E> {
    private final TransactionTask<T, E> delegate;
    private final TimelockService timelockService;
    private final LockToken immutableTsLock;

    public LockCheckingTransactionTask(TransactionTask<T, E> delegate,
            TimelockService timelockService,
            LockToken immutableTsLock) {
        this.delegate = delegate;
        this.timelockService = timelockService;
        this.immutableTsLock = immutableTsLock;

    }

    public T execute(Transaction transaction) throws E {
        try {
            return delegate.execute(transaction);
        } catch (Exception ex) {
            if (shouldRethrowWithoutLockValidation(ex) || immutableTsLockIsValid()) {
                throw ex;
            }
            throw new TransactionLockTimeoutException(
                    "The following immutable timestamp lock is no longer valid: " + immutableTsLock);
        }
    }

    private boolean shouldRethrowWithoutLockValidation(Exception ex) {
        return ex instanceof InterruptedException || ex instanceof TransactionFailedNonRetriableException;
    }

    private boolean immutableTsLockIsValid() {
        return !timelockService.refreshLockLeases(ImmutableSet.of(immutableTsLock)).isEmpty();
    }
}
