/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
 *  <p>
 *  If a read is performed without validating immutableTs lock on a thoroughly swept table, there is no guarantee on
 *  consistency of data read; as sweep may remove data that is being read. This will not cause any correctness
 *  issues as immutableTs lock will be checked on commit time, and transaction will be aborted if lock is invalid.
 *  Although this prevents committing corrupted values, reading inconsistent data may cause tasks to throw
 *  non-retriable exceptions; causing a possible behaviour change.
 *  <p>
 *  This wrapper task will convert the exception thrown by the underlying task into a retriable lock timeout exception
 *  if immutableTs lock is not valid anymore.
 */
public class LockCheckingTransactionTask<T, E extends Exception> implements TransactionTask<T, E> {
    private final TransactionTask<T, E> delegate;
    private final TimelockService timelockService;
    private final LockToken immutableTsLock;

    public LockCheckingTransactionTask(
            TransactionTask<T, E> delegate, TimelockService timelockService, LockToken immutableTsLock) {
        this.delegate = delegate;
        this.timelockService = timelockService;
        this.immutableTsLock = immutableTsLock;
    }

    @Override
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
        return !timelockService
                .refreshLockLeases(ImmutableSet.of(immutableTsLock))
                .isEmpty();
    }
}
