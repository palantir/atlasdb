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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.cleaner.Cleaner;
import com.palantir.atlasdb.cleaner.NoOpCleaner;
import com.palantir.atlasdb.keyvalue.api.ClusterAvailabilityStatus;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.KeyValueServiceStatus;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionTask;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionTasks;
import com.palantir.atlasdb.transaction.api.Transaction.TransactionType;
import com.palantir.atlasdb.transaction.api.TransactionFailedRetriableException;
import com.palantir.atlasdb.transaction.api.TransactionReadSentinelBehavior;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.common.base.Throwables;
import com.palantir.lock.AtlasTimestampLockDescriptor;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.RemoteLockService;
import com.palantir.timestamp.TimestampService;

/* package */ class SnapshotTransactionManager extends AbstractLockAwareTransactionManager {
    private static final int NUM_RETRIES = 10;

    final KeyValueService keyValueService;
    final TransactionService transactionService;
    final TimestampService timestampService;
    final RemoteLockService lockService;
    final ConflictDetectionManager conflictDetectionManager;
    final SweepStrategyManager sweepStrategyManager;
    final LockClient lockClient;
    final Supplier<AtlasDbConstraintCheckingMode> constraintModeSupplier;
    final AtomicLong recentImmutableTs = new AtomicLong(-1L);
    final Cleaner cleaner;
    final boolean allowHiddenTableAccess;

    final List<Runnable> closingCallbacks;
    final AtomicBoolean isClosed;

    protected SnapshotTransactionManager(
            KeyValueService keyValueService,
            TimestampService timestampService,
            LockClient lockClient,
            RemoteLockService lockService,
            TransactionService transactionService,
            Supplier<AtlasDbConstraintCheckingMode> constraintModeSupplier,
            ConflictDetectionManager conflictDetectionManager,
            SweepStrategyManager sweepStrategyManager,
            Cleaner cleaner) {
        this(keyValueService, timestampService, lockClient, lockService, transactionService,
                constraintModeSupplier, conflictDetectionManager, sweepStrategyManager, cleaner, false);
    }

    protected SnapshotTransactionManager(
            KeyValueService keyValueService,
            TimestampService timestampService,
            LockClient lockClient,
            RemoteLockService lockService,
            TransactionService transactionService,
            Supplier<AtlasDbConstraintCheckingMode> constraintModeSupplier,
            ConflictDetectionManager conflictDetectionManager,
            SweepStrategyManager sweepStrategyManager,
            Cleaner cleaner,
            boolean allowHiddenTableAccess) {
        Preconditions.checkArgument(lockClient != LockClient.ANONYMOUS);
        this.keyValueService = keyValueService;
        this.timestampService = timestampService;
        this.lockService = lockService;
        this.transactionService = transactionService;
        this.conflictDetectionManager = conflictDetectionManager;
        this.sweepStrategyManager = sweepStrategyManager;
        this.lockClient = lockClient;
        this.constraintModeSupplier = constraintModeSupplier;
        this.cleaner = cleaner;
        this.allowHiddenTableAccess = allowHiddenTableAccess;
        this.closingCallbacks = new CopyOnWriteArrayList<>();
        this.isClosed = new AtomicBoolean(false);
    }

    @Override
    protected boolean shouldStopRetrying(int numTimesFailed) {
        return numTimesFailed > NUM_RETRIES;
    }

    @Override
    public <T, E extends Exception> T runTaskWithLocksThrowOnConflict(
            Iterable<HeldLocksToken> lockTokens,
            LockAwareTransactionTask<T, E> task)
            throws E, TransactionFailedRetriableException {
        checkOpen();
        Iterable<LockRefreshToken> lockRefreshTokens = Iterables.transform(lockTokens,
                new Function<HeldLocksToken, LockRefreshToken>() {
                    @Nullable
                    @Override
                    public LockRefreshToken apply(HeldLocksToken input) {
                        return input.getLockRefreshToken();
                    }
                });
        RawTransaction tx = setupRunTaskWithLocksThrowOnConflict(lockRefreshTokens);
        return finishRunTaskWithLockThrowOnConflict(tx, LockAwareTransactionTasks.asLockUnaware(task, lockTokens));
    }

    public RawTransaction setupRunTaskWithLocksThrowOnConflict(Iterable<LockRefreshToken> lockTokens) {
        long immutableLockTs = timestampService.getFreshTimestamp();
        Supplier<Long> startTimestampSupplier = getStartTimestampSupplier();
        LockDescriptor lockDesc = AtlasTimestampLockDescriptor.of(immutableLockTs);
        LockRequest lockRequest = LockRequest.builder(ImmutableSortedMap.of(lockDesc, LockMode.READ))
                .withLockedInVersionId(immutableLockTs).build();
        LockRefreshToken lock;
        try {
            lock = lockService.lock(lockClient.getClientId(), lockRequest);
        } catch (InterruptedException e) {
            throw Throwables.throwUncheckedException(e);
        }
        try {
            ImmutableList<LockRefreshToken> allTokens = ImmutableList.<LockRefreshToken>builder()
                    .add(lock)
                    .addAll(lockTokens)
                    .build();
            SnapshotTransaction transaction = createTransaction(immutableLockTs, startTimestampSupplier, allTokens);
            return new RawTransaction(transaction, lock);
        } catch (Throwable e) {
            if (lock != null) {
                lockService.unlock(lock);
            }
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
            lockService.unlock(tx.getImmutableTsLock());
        }
        if ((tx.getTransactionType() == TransactionType.AGGRESSIVE_HARD_DELETE) && !tx.isAborted()) {
            // t.getCellsToScrubImmediately() checks that t has been committed
            cleaner.scrubImmediately(this,
                    tx.delegate().getCellsToScrubImmediately(),
                    tx.delegate().getTimestamp(),
                    tx.delegate().getCommitTimestamp());
        }
        return result;
    }

    protected SnapshotTransaction createTransaction(
            long immutableLockTs,
            Supplier<Long> startTimestampSupplier,
            ImmutableList<LockRefreshToken> allTokens) {
        return new SnapshotTransaction(
                keyValueService,
                lockService,
                timestampService,
                transactionService,
                cleaner,
                startTimestampSupplier,
                conflictDetectionManager,
                sweepStrategyManager,
                getImmutableTimestampInternal(immutableLockTs),
                allTokens,
                constraintModeSupplier.get(),
                cleaner.getTransactionReadTimeoutMillis(),
                TransactionReadSentinelBehavior.THROW_EXCEPTION,
                allowHiddenTableAccess,
                timestampValidationReadCache);
    }

    @Override
    public <T, E extends Exception> T runTaskReadOnly(TransactionTask<T, E> task) throws E {
        checkOpen();
        long immutableTs = getApproximateImmutableTimestamp();
        SnapshotTransaction transaction = new SnapshotTransaction(
                keyValueService,
                lockService,
                timestampService,
                transactionService,
                NoOpCleaner.INSTANCE,
                getStartTimestampSupplier(),
                conflictDetectionManager,
                sweepStrategyManager,
                immutableTs,
                Collections.<LockRefreshToken>emptyList(),
                constraintModeSupplier.get(),
                cleaner.getTransactionReadTimeoutMillis(),
                TransactionReadSentinelBehavior.THROW_EXCEPTION,
                allowHiddenTableAccess,
                timestampValidationReadCache);
        return runTaskThrowOnConflict(task, new ReadTransaction(transaction, sweepStrategyManager));
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
     * Frees resources used by this SnapshotTransactionManager, and invokes any callbacks registered to run on close.
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
            cleaner.close();
            keyValueService.close();
            closeLockServiceIfPossible();
            for (Runnable callback : Lists.reverse(closingCallbacks)) {
                callback.run();
            }
        }
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
        return Suppliers.memoize(new Supplier<Long>() {
            @Override
            public Long get() {
                long freshTimestamp = timestampService.getFreshTimestamp();
                cleaner.punch(freshTimestamp);
                return freshTimestamp;
            }
        });
    }

    @Override
    public RemoteLockService getLockService() {
        return lockService;
    }

    /**
     * This will always return a valid ImmutableTimestmap, but it may be slightly out of date.
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
        long ts = timestampService.getFreshTimestamp();
        return getImmutableTimestampInternal(ts);
    }

    protected long getImmutableTimestampInternal(long ts) {
        Long minLocked = lockService.getMinLockedInVersionId(lockClient.getClientId());
        long ret = minLocked == null ? ts : minLocked;
        long recentTs = recentImmutableTs.get();
        while (recentTs < ret) {
            if (recentImmutableTs.compareAndSet(recentTs, ret)) {
                break;
            } else {
                recentTs = recentImmutableTs.get();
            }
        }
        return ret;
    }

    @Override
    public long getUnreadableTimestamp() {
        return cleaner.getUnreadableTimestamp();
    }

    public Cleaner getCleaner() {
        return cleaner;
    }

    public KeyValueService getKeyValueService() {
        return keyValueService;
    }

    public TimestampService getTimestampService() {
        return timestampService;
    }

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
}
