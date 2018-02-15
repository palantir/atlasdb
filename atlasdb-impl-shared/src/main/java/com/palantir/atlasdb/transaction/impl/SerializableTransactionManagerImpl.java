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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cleaner.Cleaner;
import com.palantir.atlasdb.keyvalue.api.ClusterAvailabilityStatus;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.monitoring.TimestampTracker;
import com.palantir.atlasdb.monitoring.TimestampTrackerImpl;
import com.palantir.atlasdb.sweep.queue.SweepQueueWriter;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.ConditionAwareTransactionTask;
import com.palantir.atlasdb.transaction.api.KeyValueServiceStatus;
import com.palantir.atlasdb.transaction.api.PreCommitCondition;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionFailedRetriableException;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.common.base.Throwables;
import com.palantir.exception.NotInitializedException;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockService;
import com.palantir.lock.impl.LegacyTimelockService;
import com.palantir.lock.v2.LockImmutableTimestampRequest;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;
import com.palantir.processors.AutoDelegate;
import com.palantir.timestamp.TimestampService;

@AutoDelegate(typeToExtend = SerializableTransactionManager.class)
public class SerializableTransactionManagerImpl extends AbstractTransactionManager
        implements SerializableTransactionManager {

    public static class InitializeCheckingWrapper implements AutoDelegate_SerializableTransactionManager {
        private final SerializableTransactionManager manager;
        private final Supplier<Boolean> initializationPrerequisite;

        public InitializeCheckingWrapper(SerializableTransactionManager manager,
                Supplier<Boolean> initializationPrerequisite) {
            this.manager = manager;
            this.initializationPrerequisite = initializationPrerequisite;
        }

        @Override
        public SerializableTransactionManager delegate() {
            if (!isInitialized()) {
                throw new NotInitializedException("TransactionManager");
            }

            return manager;
        }

        @Override
        public boolean isInitialized() {
            // Note that the PersistentLockService is also initialized asynchronously as part of
            // TransactionManagers.create; however, this is not required for the TransactionManager to fulfil
            // requests (note that it is not accessible from any TransactionManager implementation), so we omit
            // checking here whether it is initialized.
            return manager.getKeyValueService().isInitialized()
                    && manager.getTimelockService().isInitialized()
                    && manager.getTimestampService().isInitialized()
                    && manager.getCleaner().isInitialized()
                    && initializationPrerequisite.get();
        }

        @Override
        public LockService getLockService() {
            return manager.getLockService();
        }

        @Override
        public void registerClosingCallback(Runnable closingCallback) {
            manager.registerClosingCallback(closingCallback);
        }
    }

    public static SerializableTransactionManager create(KeyValueService keyValueService,
            TimelockService timelockService,
            LockService lockService,
            TransactionService transactionService,
            Supplier<AtlasDbConstraintCheckingMode> constraintModeSupplier,
            ConflictDetectionManager conflictDetectionManager,
            SweepStrategyManager sweepStrategyManager,
            Cleaner cleaner,
            Supplier<Boolean> initializationPrerequisite,
            boolean allowHiddenTableAccess,
            Supplier<Long> lockAcquireTimeoutMs,
            int concurrentGetRangesThreadPoolSize,
            int defaultGetRangesConcurrency,
            boolean initializeAsync,
            Supplier<Long> timestampCacheSize,
            SweepQueueWriter sweepQueueWriter) {
        TimestampTracker timestampTracker = TimestampTrackerImpl.createWithDefaultTrackers(
                timelockService, cleaner, initializeAsync);
        SerializableTransactionManager serializableTransactionManager = new SerializableTransactionManagerImpl(
                keyValueService,
                timelockService,
                lockService,
                transactionService,
                constraintModeSupplier,
                conflictDetectionManager,
                sweepStrategyManager,
                cleaner,
                allowHiddenTableAccess,
                lockAcquireTimeoutMs,
                timestampTracker,
                concurrentGetRangesThreadPoolSize,
                defaultGetRangesConcurrency,
                timestampCacheSize,
                sweepQueueWriter);

        return initializeAsync
                ? new InitializeCheckingWrapper(serializableTransactionManager, initializationPrerequisite)
                : serializableTransactionManager;
    }

    public static SerializableTransactionManager createForTest(KeyValueService keyValueService,
            TimestampService timestampService,
            LockClient lockClient,
            LockService lockService,
            TransactionService transactionService,
            Supplier<AtlasDbConstraintCheckingMode> constraintModeSupplier,
            ConflictDetectionManager conflictDetectionManager,
            SweepStrategyManager sweepStrategyManager,
            Cleaner cleaner,
            int concurrentGetRangesThreadPoolSize,
            int defaultGetRangesConcurrency,
            Supplier<Long> timestampCacheSize) {
        return new SerializableTransactionManagerImpl(keyValueService,
                new LegacyTimelockService(timestampService, lockService, lockClient),
                lockService,
                transactionService,
                constraintModeSupplier,
                conflictDetectionManager,
                sweepStrategyManager,
                cleaner,
                false,
                () -> AtlasDbConstants.DEFAULT_TRANSACTION_LOCK_ACQUIRE_TIMEOUT_MS,
                TimestampTrackerImpl.createNoOpTracker(),
                concurrentGetRangesThreadPoolSize,
                defaultGetRangesConcurrency,
                timestampCacheSize,
                SweepQueueWriter.NO_OP);
    }

    private static final int NUM_RETRIES = 10;

    final KeyValueService keyValueService;
    final TimelockService timelockService;
    final LockService lockService;
    final ConflictDetectionManager conflictDetectionManager;
    final SweepStrategyManager sweepStrategyManager;
    final Supplier<AtlasDbConstraintCheckingMode> constraintModeSupplier;
    final AtomicLong recentImmutableTs = new AtomicLong(-1L);
    final Cleaner cleaner;
    final boolean allowHiddenTableAccess;
    final ExecutorService getRangesExecutor;
    final TimestampTracker timestampTracker;
    final int defaultGetRangesConcurrency;

    final List<Runnable> closingCallbacks;
    final AtomicBoolean isClosed;

    private final TransactionFactory transactionsFactory;

    /**
     * @deprecated Use {@link SerializableTransactionManagerImpl#create} to create this class.
     */
    @Deprecated
    // Used by internal product.
    public SerializableTransactionManagerImpl(KeyValueService keyValueService,
            TimestampService timestampService,
            LockClient lockClient,
            LockService lockService,
            TransactionService transactionService,
            Supplier<AtlasDbConstraintCheckingMode> constraintModeSupplier,
            ConflictDetectionManager conflictDetectionManager,
            SweepStrategyManager sweepStrategyManager,
            Cleaner cleaner,
            boolean allowHiddenTableAccess,
            TimestampTracker timestampTracker,
            int concurrentGetRangesThreadPoolSize,
            int defaultGetRangesConcurrency,
            Supplier<Long> timestampCacheSize) {
        this(
                keyValueService,
                new LegacyTimelockService(timestampService, lockService, lockClient),
                lockService,
                transactionService,
                constraintModeSupplier,
                conflictDetectionManager,
                sweepStrategyManager,
                cleaner,
                allowHiddenTableAccess, () -> AtlasDbConstants.DEFAULT_TRANSACTION_LOCK_ACQUIRE_TIMEOUT_MS,
                timestampTracker,
                concurrentGetRangesThreadPoolSize, defaultGetRangesConcurrency, timestampCacheSize,
                SweepQueueWriter.NO_OP
        );
    }

    // Canonical constructor.
    public SerializableTransactionManagerImpl(KeyValueService keyValueService,
            TimelockService timelockService,
            LockService lockService,
            TransactionService transactionService,
            Supplier<AtlasDbConstraintCheckingMode> constraintModeSupplier,
            ConflictDetectionManager conflictDetectionManager,
            SweepStrategyManager sweepStrategyManager,
            Cleaner cleaner,
            boolean allowHiddenTableAccess,
            Supplier<Long> lockAcquireTimeoutMs,
            TimestampTracker timestampTracker,
            int concurrentGetRangesThreadPoolSize,
            int defaultGetRangesConcurrency,
            Supplier<Long> timestampCacheSize,
            SweepQueueWriter sweepQueueWriter) {
        super(timestampCacheSize);

        this.keyValueService = keyValueService;
        this.timelockService = timelockService;
        this.lockService = lockService;
        this.conflictDetectionManager = conflictDetectionManager;
        this.sweepStrategyManager = sweepStrategyManager;
        this.constraintModeSupplier = constraintModeSupplier;
        this.cleaner = cleaner;
        this.allowHiddenTableAccess = allowHiddenTableAccess;
        this.closingCallbacks = new CopyOnWriteArrayList<>();
        this.isClosed = new AtomicBoolean(false);
        this.getRangesExecutor = createGetRangesExecutor(concurrentGetRangesThreadPoolSize);
        this.timestampTracker = timestampTracker;
        this.defaultGetRangesConcurrency = defaultGetRangesConcurrency;

        transactionsFactory = new TransactionFactory(keyValueService,
                timelockService,
                transactionService,
                cleaner,
                conflictDetectionManager,
                sweepStrategyManager,
                constraintModeSupplier,
                allowHiddenTableAccess,
                timestampValidationReadCache,
                lockAcquireTimeoutMs,
                getRangesExecutor,
                defaultGetRangesConcurrency,
                sweepQueueWriter);
    }

    @Override
    protected boolean shouldStopRetrying(int numTimesFailed) {
        return numTimesFailed > NUM_RETRIES;
    }

    @Override
    public <T, C extends PreCommitCondition, E extends Exception> T runTaskWithConditionThrowOnConflict(
            C condition, ConditionAwareTransactionTask<T, C, E> task)
            throws E, TransactionFailedRetriableException {
        checkOpen();
        try {
            RawTransaction tx = setupRunTaskWithConditionThrowOnConflict(condition);
            return finishRunTaskWithLockThrowOnConflict(tx, transaction -> task.execute(transaction, condition));
        } finally {
            condition.cleanup();
        }
    }

    public RawTransaction setupRunTaskWithConditionThrowOnConflict(PreCommitCondition condition) {
        LockImmutableTimestampResponse immutableTsResponse = timelockService.lockImmutableTimestamp(
                LockImmutableTimestampRequest.create());
        try {
            LockToken immutableTsLock = immutableTsResponse.getLock();
            long immutableTs = immutableTsResponse.getImmutableTimestamp();
            recordImmutableTimestamp(immutableTs);
            Supplier<Long> startTimestampSupplier = getStartTimestampSupplier();

            SnapshotTransaction transaction = transactionsFactory.createSerializableTransaction(immutableTs,
                    startTimestampSupplier,
                    immutableTsLock,
                    condition);
            return new RawTransaction(transaction, immutableTsLock);
        } catch (Throwable e) {
            timelockService.unlock(ImmutableSet.of(immutableTsResponse.getLock()));
            throw Throwables.rewrapAndThrowUncheckedException(e);
        }
    }

    public <T, E extends Exception> T finishRunTaskWithLockThrowOnConflict(RawTransaction tx,
            TransactionTask<T, E> task)
            throws E, TransactionFailedRetriableException {
        T result;
        try {
            result = runTaskThrowOnConflict(task, tx);
        } finally {
            timelockService.unlock(ImmutableSet.of(tx.getImmutableTsLock()));
        }
        if ((tx.getTransactionType() == Transaction.TransactionType.AGGRESSIVE_HARD_DELETE) && !tx.isAborted()) {
            // t.getCellsToScrubImmediately() checks that t has been committed
            cleaner.scrubImmediately(this,
                    tx.delegate().getCellsToScrubImmediately(),
                    tx.delegate().getTimestamp(),
                    tx.delegate().getCommitTimestamp());
        }
        return result;
    }



    @Override
    public <T, C extends PreCommitCondition, E extends Exception> T runTaskWithConditionReadOnly(
            C condition, ConditionAwareTransactionTask<T, C, E> task) throws E {
        checkOpen();
        Supplier<Long> startTimestampSupplier = getStartTimestampSupplier();
        SnapshotTransaction transaction = transactionsFactory.createReadOnlyTransaction(condition,
                startTimestampSupplier,
                getApproximateImmutableTimestamp());
        try {
            return runTaskThrowOnConflict(txn -> task.execute(txn, condition),
                    new ReadTransaction(transaction, sweepStrategyManager));
        } finally {
            condition.cleanup();
        }
    }

    /**
     * Registers a Runnable that will be run when the transaction manager is closed, provided no callback already
     * submitted throws an exception.
     *
     * Concurrency: If this method races with close(), then closingCallback may not be called.
     */
    public void registerClosingCallback(Runnable closingCallback) {
        Preconditions.checkNotNull(closingCallback, "Cannot register a null callback.");
        closingCallbacks.add(closingCallback);
    }

    /**
     * Frees resources used by this SerializableTransactionManager, and invokes any callbacks registered to run on
     * close.
     * This includes the cleaner, the key value service (and attendant thread pools), and possibly the lock service.
     *
     * Concurrency: If this method races with registerClosingCallback(closingCallback), then closingCallback
     * may be called (but is not necessarily called). Callbacks registered before the invocation of close() are
     * guaranteed to be executed (because we use a synchronized list) as long as no exceptions arise. If an exception
     * arises, then no guarantees are made with regard to subsequent callbacks being executed.
     */
    @Override
    public void close() {
        if (isClosed.compareAndSet(false, true)) {
            super.close();
            timestampTracker.close();
            cleaner.close();
            keyValueService.close();
            getRangesExecutor.shutdown();
            closeLockServiceIfPossible();
            for (Runnable callback : Lists.reverse(closingCallbacks)) {
                callback.run();
            }
        }
    }

    @Override
    public void clearTimestampCache() {
        timestampValidationReadCache.clear();
    }

    private void closeLockServiceIfPossible() {
        if (lockService instanceof AutoCloseable) {
            try {
                ((AutoCloseable) lockService).close();
            } catch (Exception e) {
                throw Throwables.rewrapAndThrowUncheckedException("Exception when closing the lock service", e);
            }
        }
    }

    private Supplier<Long> getStartTimestampSupplier() {
        return Suppliers.memoize(() -> {
            long freshTimestamp = timelockService.getFreshTimestamp();
            cleaner.punch(freshTimestamp);
            return freshTimestamp;
        });
    }

    @Override
    public LockService getLockService() {
        return lockService;
    }

    @Override
    public TimelockService getTimelockService() {
        return timelockService;
    }

    /**
     * This will always return a valid ImmutableTimestamp, but it may be slightly out of date.
     * <p>
     * This method is used to optimize the perf of read only transactions because getting a new immutableTs requires
     * 2 extra remote calls which we can skip.
     */
    private long getApproximateImmutableTimestamp() {
        long recentTs = recentImmutableTs.get();
        if (recentTs >= 0) {
            return recentTs;
        }
        return getImmutableTimestamp();
    }

    @Override
    public long getImmutableTimestamp() {
        long immutableTs = timelockService.getImmutableTimestamp();
        recordImmutableTimestamp(immutableTs);
        return immutableTs;
    }

    private void recordImmutableTimestamp(long immutableTs) {
        recentImmutableTs.updateAndGet(current -> Math.max(current, immutableTs));
    }

    @Override
    public long getUnreadableTimestamp() {
        return cleaner.getUnreadableTimestamp();
    }

    public Cleaner getCleaner() {
        return cleaner;
    }

    @Override
    public KeyValueService getKeyValueService() {
        return keyValueService;
    }

    public TimestampService getTimestampService() {
        return new TimelockTimestampServiceAdapter(timelockService);
    }

    @Override
    public KeyValueServiceStatus getKeyValueServiceStatus() {
        ClusterAvailabilityStatus clusterAvailabilityStatus = keyValueService.getClusterAvailabilityStatus();
        switch (clusterAvailabilityStatus) {
            case TERMINAL:
                return KeyValueServiceStatus.TERMINAL;
            case ALL_AVAILABLE:
                return KeyValueServiceStatus.HEALTHY_ALL_OPERATIONS;
            case QUORUM_AVAILABLE:
                return KeyValueServiceStatus.HEALTHY_BUT_NO_SCHEMA_MUTATIONS_OR_DELETES;
            case NO_QUORUM_AVAILABLE:
            default:
                return KeyValueServiceStatus.UNHEALTHY;
        }
    }

    @VisibleForTesting
    ConflictDetectionManager getConflictDetectionManager() {
        return conflictDetectionManager;
    }
}

