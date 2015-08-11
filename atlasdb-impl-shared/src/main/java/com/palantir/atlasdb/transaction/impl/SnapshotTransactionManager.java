/**
 * Copyright 2015 Palantir Technologies
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
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.palantir.atlasdb.cleaner.Cleaner;
import com.palantir.atlasdb.cleaner.NoOpCleaner;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionTask;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionTasks;
import com.palantir.atlasdb.transaction.api.Transaction.TransactionType;
import com.palantir.atlasdb.transaction.api.TransactionFailedRetriableException;
import com.palantir.atlasdb.transaction.api.TransactionReadSentinelBehavior;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.common.base.Throwables;
import com.palantir.lock.AtlasTimestampLockDescriptor;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.RemoteLockService;
import com.palantir.timestamp.TimestampService;

public class SnapshotTransactionManager extends AbstractLockAwareTransactionManager {
    private final static int NUM_RETRIES = 10;

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

    public SnapshotTransactionManager(KeyValueService keyValueService,
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

    public SnapshotTransactionManager(KeyValueService keyValueService,
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
    }

    @Override
    protected boolean shouldStopRetrying(int numTimesFailed) {
        return numTimesFailed > NUM_RETRIES;
    }

    @Override
    public <T, E extends Exception> T runTaskWithLocksThrowOnConflict(Iterable<LockRefreshToken> lockTokens,
                                                                      LockAwareTransactionTask<T, E> task)
            throws E, TransactionFailedRetriableException {
        long immutableLockTs = timestampService.getFreshTimestamp();
        Supplier<Long> startTimestampSupplier = getStartTimestampSupplier();
        LockDescriptor lockDesc = AtlasTimestampLockDescriptor.of(immutableLockTs);
        LockRequest lockRequest =
                LockRequest.builder(ImmutableSortedMap.of(lockDesc, LockMode.READ)).withLockedInVersionId(
                        immutableLockTs).build();
        final LockRefreshToken lock;
        final T result;
        final SnapshotTransaction t;
        try {
            lock = lockService.lockWithClient(lockClient.getClientId(), lockRequest);
        } catch (InterruptedException e) {
            throw Throwables.throwUncheckedException(e);
        }
        try {
            ImmutableList<LockRefreshToken> allTokens =
                    ImmutableList.<LockRefreshToken> builder().add(lock).addAll(lockTokens).build();
            t = createTransaction(immutableLockTs, startTimestampSupplier, allTokens);
            result = runTaskThrowOnConflict(LockAwareTransactionTasks.asLockUnaware(task, lockTokens), t);
        } finally {
            lockService.unlock(lock);
        }
        if (t.getTransactionType() == TransactionType.AGGRESSIVE_HARD_DELETE) {
            // t.getCellsToScrubImmediately() checks that t has been committed
            cleaner.scrubImmediately(this, t.getCellsToScrubImmediately(), t.getTimestamp(), t.getCommitTimestamp());
        }
        return result;
    }

    protected SnapshotTransaction createTransaction(long immutableLockTs,
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
                allowHiddenTableAccess);
    }

    @Override
    public <T, E extends Exception> T runTaskReadOnly(TransactionTask<T, E> task) throws E {
        long immutableTs = getApproximateImmutableTimestamp();
        SnapshotTransaction t = new SnapshotTransaction(
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
                allowHiddenTableAccess);
        return runTaskThrowOnConflict(task, new OnlyWriteTempTablesTransaction(t, sweepStrategyManager));
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

    public static void createTables(KeyValueService keyValueService) {
        keyValueService.createTable(TransactionConstants.TRANSACTION_TABLE, TransactionConstants.getValueForTimestamp(-1).length);
        keyValueService.putMetadataForTable(TransactionConstants.TRANSACTION_TABLE, TransactionConstants.TRANSACTION_TABLE_METADATA.persistToBytes());
    }

    public static void deleteTables(KeyValueService keyValueService) {
        keyValueService.dropTable(TransactionConstants.TRANSACTION_TABLE);
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
}
