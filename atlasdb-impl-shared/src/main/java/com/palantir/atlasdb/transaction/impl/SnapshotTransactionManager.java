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

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
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
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.RemoteLockService;
import com.palantir.lock.v2.LockImmutableTimestampRequest;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockTokenV2;
import com.palantir.lock.v2.TimelockService;
import com.palantir.timestamp.TimestampService;

/* package */ class SnapshotTransactionManager extends AbstractLockAwareTransactionManager {
    private static final int NUM_RETRIES = 10;

    final KeyValueService keyValueService;
    final TransactionService transactionService;
    final TimelockService timelockService;
    final Optional<RemoteLockService> lockService;
    final ConflictDetectionManager conflictDetectionManager;
    final SweepStrategyManager sweepStrategyManager;
    final Supplier<AtlasDbConstraintCheckingMode> constraintModeSupplier;
    final AtomicLong recentImmutableTs = new AtomicLong(-1L);
    final Cleaner cleaner;
    final boolean allowHiddenTableAccess;

    protected SnapshotTransactionManager(
            KeyValueService keyValueService,
            TimelockService timelockService,
            Optional<RemoteLockService> lockService,
            TransactionService transactionService,
            Supplier<AtlasDbConstraintCheckingMode> constraintModeSupplier,
            ConflictDetectionManager conflictDetectionManager,
            SweepStrategyManager sweepStrategyManager,
            Cleaner cleaner,
            boolean allowHiddenTableAccess) {
        this.keyValueService = keyValueService;
        this.timelockService = timelockService;
        this.lockService = lockService;
        this.transactionService = transactionService;
        this.conflictDetectionManager = conflictDetectionManager;
        this.sweepStrategyManager = sweepStrategyManager;
        this.constraintModeSupplier = constraintModeSupplier;
        this.cleaner = cleaner;
        this.allowHiddenTableAccess = allowHiddenTableAccess;
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
        LockImmutableTimestampResponse immutableTsResponse = timelockService.lockImmutableTimestamp(
                LockImmutableTimestampRequest.create());
        try {
            LockTokenV2 immutableTsLock = immutableTsResponse.getLock();
            long immutableTs = immutableTsResponse.getImmutableTimestamp();
            recordImmutableTimestamp(immutableTsResponse.getImmutableTimestamp());
            Supplier<Long> startTimestampSupplier = getStartTimestampSupplier();

            PreCommitValidation preCommitCheck = PreCommitValidation.forLockServiceLocks(lockTokens, getLockService());
            SnapshotTransaction transaction = createTransaction(immutableTs, startTimestampSupplier,
                    ImmutableList.of(immutableTsLock), preCommitCheck);
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
            long immutableTimestamp,
            Supplier<Long> startTimestampSupplier,
            ImmutableList<LockTokenV2> allTokens,
            PreCommitValidation preCommitValidation) {
        return new SnapshotTransaction(
                keyValueService,
                timelockService,
                transactionService,
                cleaner,
                startTimestampSupplier,
                conflictDetectionManager,
                sweepStrategyManager,
                immutableTimestamp,
                allTokens,
                preCommitValidation,
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
                timelockService,
                transactionService,
                NoOpCleaner.INSTANCE,
                getStartTimestampSupplier(),
                conflictDetectionManager,
                sweepStrategyManager,
                immutableTs,
                ImmutableList.of(),
                PreCommitValidation.NO_OP,
                constraintModeSupplier.get(),
                cleaner.getTransactionReadTimeoutMillis(),
                TransactionReadSentinelBehavior.THROW_EXCEPTION,
                allowHiddenTableAccess,
                timestampValidationReadCache);
        return runTaskThrowOnConflict(task, new ReadTransaction(transaction, sweepStrategyManager));
    }

    @Override
    public void close() {
        super.close();
        cleaner.close();
        keyValueService.close();
    }

    private Supplier<Long> getStartTimestampSupplier() {
        return Suppliers.memoize(() -> {
            long freshTimestamp = timelockService.getFreshTimestamp();
            cleaner.punch(freshTimestamp);
            return freshTimestamp;
        });
    }

    @Override
    public RemoteLockService getLockService() {
        Preconditions.checkState(
                lockService.isPresent(),
                "This transaction manager has not been configured with a RemoteLockService. "
                        + "This is likely because you are using Timelock server, which does not support RemoteLockService.");
        return lockService.get();
    }

    @Override
    public TimelockService getTimelockService() {
        return timelockService;
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
        long immutableTs = timelockService.getImmutableTimestamp();
        recordImmutableTimestamp(immutableTs);
        return immutableTs;
    }

    private void recordImmutableTimestamp(long immutableTs) {
        long recentTs = recentImmutableTs.get();
        while (recentTs < immutableTs) {
            if (recentImmutableTs.compareAndSet(recentTs, immutableTs)) {
                break;
            } else {
                recentTs = recentImmutableTs.get();
            }
        }
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
        return new TimelockTimestampServiceAdapter(timelockService);
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
