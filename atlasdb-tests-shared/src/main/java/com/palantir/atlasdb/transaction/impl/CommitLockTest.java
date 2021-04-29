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

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.cleaner.NoOpCleaner;
import com.palantir.atlasdb.debug.ConflictTracer;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.watch.NoOpLockWatchManager;
import com.palantir.atlasdb.keyvalue.impl.TestResourceManager;
import com.palantir.atlasdb.sweep.queue.MultiTableSweepQueueWriter;
import com.palantir.atlasdb.transaction.ImmutableTransactionConfig;
import com.palantir.atlasdb.transaction.TransactionConfig;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.api.PreCommitCondition;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionReadSentinelBehavior;
import com.palantir.atlasdb.transaction.impl.metrics.SimpleTableLevelMetricsController;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.lock.AtlasCellLockDescriptor;
import com.palantir.lock.AtlasRowLockDescriptor;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import java.util.Map;
import java.util.Optional;
import org.junit.Assume;
import org.junit.ClassRule;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

@RunWith(Theories.class)
public class CommitLockTest extends TransactionTestSetup {
    @ClassRule
    public static final TestResourceManager TRM = TestResourceManager.inMemory();

    private static final TransactionConfig TRANSACTION_CONFIG =
            ImmutableTransactionConfig.builder().build();
    private static final String ROW = "row";
    private static final String COLUMN = "col_1";
    private static final String OTHER_COLUMN = "col_2";

    @DataPoints
    public static ConflictHandler[] conflictHandlers = ConflictHandler.values();

    public CommitLockTest() {
        super(TRM, TRM);
    }

    @Theory
    public void shouldAcquireRowLockIfLocksAtRowLevel(ConflictHandler conflictHandler) {
        Assume.assumeTrue(conflictHandler.lockRowsForConflicts());

        PreCommitCondition rowLocksAcquired = ignored -> {
            LockResponse response = acquireRowLock(ROW);
            assertThat(response.wasSuccessful()).isFalse();
        };

        commitWriteWith(rowLocksAcquired, conflictHandler);
    }

    @Theory
    public void shouldAcquireCellLockIfLocksAtCellLevel(ConflictHandler conflictHandler) {
        Assume.assumeTrue(conflictHandler.lockCellsForConflicts());

        PreCommitCondition cellLocksAcquired = ignored -> {
            LockResponse response = acquireCellLock(ROW, COLUMN);
            assertThat(response.wasSuccessful()).isFalse();
        };

        commitWriteWith(cellLocksAcquired, conflictHandler);
    }

    @Theory
    public void shouldNotAcquireRowLockIfDoesNotLockAtRowLevel(ConflictHandler conflictHandler) {
        Assume.assumeFalse(conflictHandler.lockRowsForConflicts());

        PreCommitCondition canAcquireRowLock = ignored -> {
            LockResponse response = acquireRowLock(ROW);
            assertThat(response.wasSuccessful()).isTrue();
        };

        commitWriteWith(canAcquireRowLock, conflictHandler);
    }

    @Theory
    public void shouldNotAcquireCellLockIfDoesNotLockAtCellLevel(ConflictHandler conflictHandler) {
        Assume.assumeFalse(conflictHandler.lockCellsForConflicts());

        PreCommitCondition canAcquireCellLock = ignored -> {
            LockResponse response = acquireCellLock(ROW, COLUMN);
            // current lock implementation allows you to get a cell lock on a row that is already locked
            assertThat(response.wasSuccessful()).isTrue();
        };

        commitWriteWith(canAcquireCellLock, conflictHandler);
    }

    @Theory
    public void shouldAcquireRowAndCellLockIfRequiresBoth(ConflictHandler conflictHandler) {
        Assume.assumeTrue(conflictHandler.lockCellsForConflicts() && conflictHandler.lockRowsForConflicts());

        PreCommitCondition cellAndRowLockAcquired = ignored -> {
            LockResponse cellLockResponse = acquireCellLock(ROW, COLUMN);
            LockResponse rowLockResponse = acquireRowLock(ROW);

            assertThat(cellLockResponse.wasSuccessful()).isFalse();
            assertThat(rowLockResponse.wasSuccessful()).isFalse();
        };

        commitWriteWith(cellAndRowLockAcquired, conflictHandler);
    }

    @Theory
    public void canAcquireLockOnMultipleCellsOnSameRow(ConflictHandler conflictHandler) {
        Assume.assumeTrue(conflictHandler.lockCellsForConflicts());

        PreCommitCondition canAcquireLockOnDifferentCell = ignored -> {
            LockResponse response = acquireCellLock(ROW, OTHER_COLUMN);
            assertThat(response.wasSuccessful()).isTrue();
        };

        commitWriteWith(canAcquireLockOnDifferentCell, conflictHandler);
    }

    private void commitWriteWith(PreCommitCondition preCommitCondition, ConflictHandler conflictHandler) {
        Transaction transaction = startTransaction(preCommitCondition, conflictHandler);
        put(transaction, ROW, COLUMN, "100");
        transaction.commit();
    }

    private Transaction startTransaction(PreCommitCondition preCommitCondition, ConflictHandler conflictHandler) {
        ImmutableMap<TableReference, ConflictHandler> tablesToWriteWrite = ImmutableMap.of(
                TEST_TABLE, conflictHandler, TransactionConstants.TRANSACTION_TABLE, ConflictHandler.IGNORE_ALL);
        return new SerializableTransaction(
                MetricsManagers.createForTests(),
                keyValueService,
                timelockService,
                NoOpLockWatchManager.create(),
                transactionService,
                NoOpCleaner.INSTANCE,
                Suppliers.ofInstance(timestampService.getFreshTimestamp()),
                TestConflictDetectionManagers.createWithStaticConflictDetection(tablesToWriteWrite),
                SweepStrategyManagers.createDefault(keyValueService),
                0L,
                Optional.empty(),
                preCommitCondition,
                AtlasDbConstraintCheckingMode.NO_CONSTRAINT_CHECKING,
                null,
                TransactionReadSentinelBehavior.THROW_EXCEPTION,
                true,
                timestampCache,
                AbstractTransactionTest.GET_RANGES_EXECUTOR,
                AbstractTransactionTest.DEFAULT_GET_RANGES_CONCURRENCY,
                MultiTableSweepQueueWriter.NO_OP,
                MoreExecutors.newDirectExecutorService(),
                true,
                () -> TRANSACTION_CONFIG,
                ConflictTracer.NO_OP,
                new SimpleTableLevelMetricsController(metricsManager)) {
            @Override
            protected Map<Cell, byte[]> transformGetsForTesting(Map<Cell, byte[]> map) {
                return Maps.transformValues(map, byte[]::clone);
            }
        };
    }

    private LockResponse acquireRowLock(String rowName) {
        LockDescriptor rowLockDescriptor =
                AtlasRowLockDescriptor.of(TEST_TABLE.getQualifiedName(), PtBytes.toBytes(rowName));
        return lock(rowLockDescriptor);
    }

    private LockResponse acquireCellLock(String rowName, String colName) {
        LockDescriptor cellLockDescriptor = AtlasCellLockDescriptor.of(
                TEST_TABLE.getQualifiedName(), PtBytes.toBytes(rowName), PtBytes.toBytes(colName));
        return lock(cellLockDescriptor);
    }

    private LockResponse lock(LockDescriptor lockDescriptor) {
        LockRequest lockRequest = LockRequest.of(ImmutableSet.of(lockDescriptor), 5_000);
        return timelockService.lock(lockRequest);
    }
}
