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

import java.util.concurrent.ExecutorService;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ClusterAvailabilityStatus;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.ConditionAwareTransactionTask;
import com.palantir.atlasdb.transaction.api.KeyValueServiceStatus;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionTask;
import com.palantir.atlasdb.transaction.api.PreCommitCondition;
import com.palantir.atlasdb.transaction.api.TransactionFailedRetriableException;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.api.TransactionReadSentinelBehavior;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockService;
import com.palantir.lock.v2.TimelockService;

/**
 * This {@link TransactionManager} will provide transactions that will read the most recently
 * committed values stored by a {@link SnapshotTransactionManager}. This does not provide snapshot
 * isolation but will always read the most recently committed value for any {@link Cell}.
 */
public class ReadOnlyTransactionManager extends AbstractLockAwareTransactionManager  {
    protected final KeyValueService keyValueService;
    protected final TransactionService transactionService;
    protected final AtlasDbConstraintCheckingMode constraintCheckingMode;
    protected final Supplier<Long> startTimestamp;
    protected final TransactionReadSentinelBehavior readSentinelBehavior;
    protected final boolean allowHiddenTableAccess;
    final ExecutorService getRangesExecutor;
    final int defaultGetRangesConcurrency;

    public ReadOnlyTransactionManager(KeyValueService keyValueService,
            TransactionService transactionService,
            AtlasDbConstraintCheckingMode constraintCheckingMode,
            int concurrentGetRangesThreadPoolSize,
            int defaultGetRangesConcurrency,
            Supplier<Long> timestampCacheSize) {
        this(
                keyValueService,
                transactionService,
                constraintCheckingMode,
                Suppliers.ofInstance(Long.MAX_VALUE),
                TransactionReadSentinelBehavior.THROW_EXCEPTION,
                false,
                concurrentGetRangesThreadPoolSize,
                defaultGetRangesConcurrency,
                timestampCacheSize);
    }

    public ReadOnlyTransactionManager(KeyValueService keyValueService,
            TransactionService transactionService,
            AtlasDbConstraintCheckingMode constraintCheckingMode,
            Supplier<Long> startTimestamp,
            TransactionReadSentinelBehavior readSentinelBehavior,
            int concurrentGetRangesThreadPoolSize,
            int defaultGetRangesConcurrency,
            Supplier<Long> timestampCacheSize) {
        this(
                keyValueService,
                transactionService,
                constraintCheckingMode,
                startTimestamp,
                readSentinelBehavior,
                false,
                concurrentGetRangesThreadPoolSize,
                defaultGetRangesConcurrency,
                timestampCacheSize);
    }

    public ReadOnlyTransactionManager(KeyValueService keyValueService,
            TransactionService transactionService,
            AtlasDbConstraintCheckingMode constraintCheckingMode,
            Supplier<Long> startTimestamp,
            TransactionReadSentinelBehavior readSentinelBehavior,
            boolean allowHiddenTableAccess,
            int concurrentGetRangesThreadPoolSize,
            int defaultGetRangesConcurrency,
            Supplier<Long> timestampCacheSize) {
        super(timestampCacheSize::get);
        this.keyValueService = keyValueService;
        this.transactionService = transactionService;
        this.constraintCheckingMode = constraintCheckingMode;
        this.startTimestamp = startTimestamp;
        this.readSentinelBehavior = readSentinelBehavior;
        this.allowHiddenTableAccess = allowHiddenTableAccess;
        this.getRangesExecutor = createGetRangesExecutor(concurrentGetRangesThreadPoolSize);
        this.defaultGetRangesConcurrency = defaultGetRangesConcurrency;
    }

    @Override
    public <T, E extends Exception> T runTaskReadOnly(TransactionTask<T, E> task) throws E {
        return runTaskReadOnlyWithCondition(NO_OP_CONDITION, (txn, condition) -> task.execute(txn));
    }

    @Override
    public void close() {
        super.close();
        keyValueService.close();
    }

    @Override
    public <T, E extends Exception> T runTaskThrowOnConflict(TransactionTask<T, E> task) throws E,
            TransactionFailedRetriableException {
        throw new UnsupportedOperationException("this manager is read only");
    }

    @Override
    public <T, E extends Exception> T runTaskWithLocksWithRetry(
            Supplier<LockRequest> lockSupplier,
            LockAwareTransactionTask<T, E> task)
            throws E, InterruptedException {
        throw new UnsupportedOperationException("this manager is read only");
    }

    @Override
    public <T, E extends Exception> T runTaskWithLocksWithRetry(
            Iterable<HeldLocksToken> lockTokens,
            Supplier<LockRequest> lockSupplier,
            LockAwareTransactionTask<T, E> task)
            throws E, InterruptedException {
        throw new UnsupportedOperationException("this manager is read only");
    }

    @Override
    public <T, E extends Exception> T runTaskWithLocksThrowOnConflict(
            Iterable<HeldLocksToken> lockTokens,
            LockAwareTransactionTask<T, E> task)
            throws E, TransactionFailedRetriableException {
        throw new UnsupportedOperationException("this manager is read only");
    }

    @Override
    public long getImmutableTimestamp() {
        return Long.MAX_VALUE;
    }

    @Override
    public KeyValueServiceStatus getKeyValueServiceStatus() {
        ClusterAvailabilityStatus clusterAvailabilityStatus = keyValueService.getClusterAvailabilityStatus();
        switch (clusterAvailabilityStatus) {
            case ALL_AVAILABLE:
            case QUORUM_AVAILABLE:
                return KeyValueServiceStatus.HEALTHY_ALL_OPERATIONS;
            case NO_QUORUM_AVAILABLE:
                return KeyValueServiceStatus.UNHEALTHY;
            case TERMINAL:
                return KeyValueServiceStatus.TERMINAL;
            default:
                log.warn("The kvs returned a non-standard availability status: {}", clusterAvailabilityStatus);
                return KeyValueServiceStatus.UNHEALTHY;
        }
    }

    @Override
    public long getUnreadableTimestamp() {
        return Long.MAX_VALUE;
    }

    @Override
    public void clearTimestampCache() {}

    @Override
    public LockService getLockService() {
        return null;
    }

    @Override
    public TimelockService getTimelockService() {
        return null;
    }

    @Override
    public <T, C extends PreCommitCondition, E extends Exception> T runTaskWithConditionThrowOnConflict(C condition,
            ConditionAwareTransactionTask<T, C, E> task) throws E, TransactionFailedRetriableException {
        throw new UnsupportedOperationException("this manager is read only");
    }

    @Override
    public <T, C extends PreCommitCondition, E extends Exception> T runTaskReadOnlyWithCondition(C condition,
            ConditionAwareTransactionTask<T, C, E> task) throws E {
        checkOpen();
        SnapshotTransaction txn = new ShouldNotDeleteAndRollbackTransaction(
                keyValueService,
                transactionService,
                startTimestamp.get(),
                constraintCheckingMode,
                readSentinelBehavior,
                allowHiddenTableAccess,
                timestampValidationReadCache,
                getRangesExecutor,
                defaultGetRangesConcurrency);
        return runTaskThrowOnConflict((transaction) -> task.execute(transaction, condition),
                new ReadTransaction(txn, txn.sweepStrategyManager));
    }
}
