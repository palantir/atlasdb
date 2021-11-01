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

import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.cache.TimestampCache;
import com.palantir.atlasdb.cleaner.api.Cleaner;
import com.palantir.atlasdb.keyvalue.api.ClusterAvailabilityStatus;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.watch.LockWatchManager;
import com.palantir.atlasdb.transaction.TransactionConfig;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.ConditionAwareTransactionTask;
import com.palantir.atlasdb.transaction.api.KeyValueServiceStatus;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionTask;
import com.palantir.atlasdb.transaction.api.OpenTransaction;
import com.palantir.atlasdb.transaction.api.PreCommitCondition;
import com.palantir.atlasdb.transaction.api.TransactionFailedRetriableException;
import com.palantir.atlasdb.transaction.api.TransactionReadSentinelBehavior;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockService;
import com.palantir.lock.v2.TimelockService;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.timestamp.TimestampManagementService;
import com.palantir.timestamp.TimestampService;
import java.util.List;
import java.util.function.Supplier;

public final class ReadOnlyTransactionManager extends AbstractLockAwareTransactionManager {
    private static final SafeLogger log = SafeLoggerFactory.get(ReadOnlyTransactionManager.class);

    private final MetricsManager metricsManager;
    private final KeyValueService keyValueService;
    private final TransactionService transactionService;
    private final AtlasDbConstraintCheckingMode constraintCheckingMode;
    private final Supplier<Long> startTimestamp;
    private final TransactionReadSentinelBehavior readSentinelBehavior;
    private final boolean allowHiddenTableAccess;
    private final int defaultGetRangesConcurrency;
    private final Supplier<TransactionConfig> transactionConfig;

    public ReadOnlyTransactionManager(
            MetricsManager metricsManager,
            KeyValueService keyValueService,
            TransactionService transactionService,
            AtlasDbConstraintCheckingMode constraintCheckingMode,
            Supplier<Long> startTimestamp,
            TransactionReadSentinelBehavior readSentinelBehavior,
            boolean allowHiddenTableAccess,
            int defaultGetRangesConcurrency,
            TimestampCache timestampCache,
            Supplier<TransactionConfig> transactionConfig) {
        super(metricsManager, timestampCache, () -> transactionConfig.get().retryStrategy());
        this.metricsManager = metricsManager;
        this.keyValueService = keyValueService;
        this.transactionService = transactionService;
        this.constraintCheckingMode = constraintCheckingMode;
        this.startTimestamp = startTimestamp;
        this.readSentinelBehavior = readSentinelBehavior;
        this.allowHiddenTableAccess = allowHiddenTableAccess;
        this.defaultGetRangesConcurrency = defaultGetRangesConcurrency;
        this.transactionConfig = transactionConfig;
    }

    @Override
    public <T, E extends Exception> T runTaskReadOnly(TransactionTask<T, E> task) throws E {
        return runTaskWithConditionReadOnly(NO_OP_CONDITION, (txn, _condition) -> task.execute(txn));
    }

    @Override
    public void close() {
        super.close();
        keyValueService.close();
    }

    @Override
    public <T, E extends Exception> T runTaskThrowOnConflict(TransactionTask<T, E> _task)
            throws E, TransactionFailedRetriableException {
        throw new UnsupportedOperationException("this manager is read only");
    }

    @Override
    public <T, E extends Exception> T runTaskWithLocksWithRetry(
            Supplier<LockRequest> _lockSupplier, LockAwareTransactionTask<T, E> _task) {
        throw new UnsupportedOperationException("this manager is read only");
    }

    @Override
    public <T, E extends Exception> T runTaskWithLocksWithRetry(
            Iterable<HeldLocksToken> _lockTokens,
            Supplier<LockRequest> _lockSupplier,
            LockAwareTransactionTask<T, E> _task) {
        throw new UnsupportedOperationException("this manager is read only");
    }

    @Override
    public <T, E extends Exception> T runTaskWithLocksThrowOnConflict(
            Iterable<HeldLocksToken> _lockTokens, LockAwareTransactionTask<T, E> _task)
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
                log.warn(
                        "The kvs returned a non-standard availability status: {}",
                        SafeArg.of("status", clusterAvailabilityStatus));
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
    public void registerClosingCallback(Runnable _closingCallback) {
        throw new UnsupportedOperationException("Not supported on this transaction manager");
    }

    @Override
    public List<OpenTransaction> startTransactions(List<? extends PreCommitCondition> _condition) {
        throw new UnsupportedOperationException("Not supported on this transaction manager");
    }

    @Override
    public LockService getLockService() {
        return null;
    }

    @Override
    public TimelockService getTimelockService() {
        return null;
    }

    @Override
    public LockWatchManager getLockWatchManager() {
        return null;
    }

    @Override
    public TimestampService getTimestampService() {
        return null;
    }

    @Override
    public TimestampManagementService getTimestampManagementService() {
        return null;
    }

    @Override
    public TransactionService getTransactionService() {
        return transactionService;
    }

    @Override
    public Cleaner getCleaner() {
        return null;
    }

    @Override
    public KeyValueService getKeyValueService() {
        return null;
    }

    @Override
    public <T, C extends PreCommitCondition, E extends Exception> T runTaskWithConditionThrowOnConflict(
            C _condition, ConditionAwareTransactionTask<T, C, E> _task) throws E, TransactionFailedRetriableException {
        throw new UnsupportedOperationException("this manager is read only");
    }

    @Override
    public <T, C extends PreCommitCondition, E extends Exception> T runTaskWithConditionReadOnly(
            C condition, ConditionAwareTransactionTask<T, C, E> task) throws E {
        checkOpen();
        SnapshotTransaction txn = new ShouldNotDeleteAndRollbackTransaction(
                metricsManager,
                keyValueService,
                transactionService,
                startTimestamp.get(),
                constraintCheckingMode,
                readSentinelBehavior,
                allowHiddenTableAccess,
                timestampValidationReadCache,
                MoreExecutors.newDirectExecutorService(),
                defaultGetRangesConcurrency,
                transactionConfig);
        return runTaskThrowOnConflict(
                transaction -> task.execute(transaction, condition),
                new ReadTransaction(txn, txn.sweepStrategyManager));
    }
}
