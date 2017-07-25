/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.factory;

import java.util.Set;

import com.google.common.base.Supplier;
import com.palantir.atlasdb.cleaner.Cleaner;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.AtlasDbRuntimeConfig;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.transaction.api.KeyValueServiceStatus;
import com.palantir.atlasdb.transaction.api.LockAcquisitionException;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionTask;
import com.palantir.atlasdb.transaction.api.TransactionFailedRetriableException;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.atlasdb.transaction.impl.RawTransaction;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;
import com.palantir.atlasdb.transaction.impl.SnapshotTransactionManagerInterface;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.RemoteLockService;
import com.palantir.timestamp.TimestampService;

public class InitialisingTransactionManager implements SnapshotTransactionManagerInterface {

    private volatile SerializableTransactionManager delegate = null;


    @Override
    public RawTransaction setupRunTaskWithLocksThrowOnConflict(Iterable<LockRefreshToken> lockTokens) {
        checkInitialised();
        return null;
    }

    @Override
    public <T, E extends Exception> T finishRunTaskWithLockThrowOnConflict(RawTransaction tx,
            TransactionTask<T, E> task) throws E, TransactionFailedRetriableException {
        checkInitialised();
        return null;
    }

    @Override
    public void registerClosingCallback(Runnable closingCallback) {
        checkInitialised();

    }

    @Override
    public Cleaner getCleaner() {
        checkInitialised();
        return null;
    }

    @Override
    public KeyValueService getKeyValueService() {
        checkInitialised();
        return null;
    }

    @Override
    public TimestampService getTimestampService() {
        checkInitialised();
        return null;
    }

    @Override
    public <T, E extends Exception> T runTaskWithRetry(TransactionTask<T, E> task) throws E {
        checkInitialised();
        return null;
    }

    @Override
    public <T, E extends Exception> T runTaskThrowOnConflict(TransactionTask<T, E> task)
            throws E, TransactionFailedRetriableException {
        checkInitialised();
        return null;
    }

    @Override
    public <T, E extends Exception> T runTaskReadOnly(TransactionTask<T, E> task) throws E {
        checkInitialised();
        return null;
    }

    @Override
    public long getImmutableTimestamp() {
        checkInitialised();
        return 0;
    }

    @Override
    public KeyValueServiceStatus getKeyValueServiceStatus() {
        checkInitialised();
        return null;
    }

    @Override
    public long getUnreadableTimestamp() {
        checkInitialised();
        return 0;
    }

    @Override
    public void clearTimestampCache() {
        checkInitialised();

    }

    @Override
    public void close() {
        checkInitialised();

    }

    @Override
    public <T, E extends Exception> T runTaskWithLocksWithRetry(Supplier<LockRequest> lockSupplier,
            LockAwareTransactionTask<T, E> task) throws E, InterruptedException, LockAcquisitionException {
        checkInitialised();
        return null;
    }

    @Override
    public <T, E extends Exception> T runTaskWithLocksWithRetry(Iterable<HeldLocksToken> lockTokens,
            Supplier<LockRequest> lockSupplier, LockAwareTransactionTask<T, E> task)
            throws E, InterruptedException, LockAcquisitionException {
        checkInitialised();
        return null;
    }

    @Override
    public <T, E extends Exception> T runTaskWithLocksThrowOnConflict(Iterable<HeldLocksToken> lockTokens,
            LockAwareTransactionTask<T, E> task) throws E, TransactionFailedRetriableException {
        checkInitialised();
        return null;
    }

    @Override
    public RemoteLockService getLockService() {
        checkInitialised();
        return null;
    }

    private void checkInitialised() {
        if (delegate == null) throw new IllegalStateException("Transaction Manager is not yet initialised!");
    }
}
