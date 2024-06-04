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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.impl.TestResourceManager;
import com.palantir.atlasdb.sweep.queue.MultiTableSweepQueueWriter;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.api.PreCommitCondition;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.lock.AtlasCellLockDescriptor;
import com.palantir.lock.AtlasRowLockDescriptor;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class CommitLockTest extends TransactionTestSetup {
    @RegisterExtension
    public static final TestResourceManager TRM = TestResourceManager.inMemory();

    private static final String ROW = "row";
    private static final String COLUMN = "col_1";
    private static final String OTHER_COLUMN = "col_2";

    private static List<ConflictHandler> conflictHandlers() {
        return Arrays.asList(ConflictHandler.values());
    }

    public CommitLockTest() {
        super(TRM, TRM);
    }

    @ParameterizedTest
    @MethodSource("conflictHandlers")
    public void shouldAcquireRowLockIfLocksAtRowLevel(ConflictHandler conflictHandler) {
        Assumptions.assumeTrue(conflictHandler.lockRowsForConflicts());

        PreCommitCondition rowLocksAcquired = ignored -> {
            LockResponse response = acquireRowLock(ROW);
            assertThat(response.wasSuccessful()).isFalse();
        };

        commitWriteWith(rowLocksAcquired, conflictHandler);
    }

    @ParameterizedTest
    @MethodSource("conflictHandlers")
    public void shouldAcquireCellLockIfLocksAtCellLevel(ConflictHandler conflictHandler) {
        Assumptions.assumeTrue(conflictHandler.lockCellsForConflicts());

        PreCommitCondition cellLocksAcquired = ignored -> {
            LockResponse response = acquireCellLock(ROW, COLUMN);
            assertThat(response.wasSuccessful()).isFalse();
        };

        commitWriteWith(cellLocksAcquired, conflictHandler);
    }

    @ParameterizedTest
    @MethodSource("conflictHandlers")
    public void shouldNotAcquireRowLockIfDoesNotLockAtRowLevel(ConflictHandler conflictHandler) {
        Assumptions.assumeFalse(conflictHandler.lockRowsForConflicts());

        PreCommitCondition canAcquireRowLock = ignored -> {
            LockResponse response = acquireRowLock(ROW);
            assertThat(response.wasSuccessful()).isTrue();
        };

        commitWriteWith(canAcquireRowLock, conflictHandler);
    }

    @ParameterizedTest
    @MethodSource("conflictHandlers")
    public void shouldNotAcquireCellLockIfDoesNotLockAtCellLevel(ConflictHandler conflictHandler) {
        Assumptions.assumeFalse(conflictHandler.lockCellsForConflicts());

        PreCommitCondition canAcquireCellLock = ignored -> {
            LockResponse response = acquireCellLock(ROW, COLUMN);
            // current lock implementation allows you to get a cell lock on a row that is already locked
            assertThat(response.wasSuccessful()).isTrue();
        };

        commitWriteWith(canAcquireCellLock, conflictHandler);
    }

    @ParameterizedTest
    @MethodSource("conflictHandlers")
    public void shouldAcquireRowAndCellLockIfRequiresBoth(ConflictHandler conflictHandler) {
        Assumptions.assumeTrue(conflictHandler.lockCellsForConflicts() && conflictHandler.lockRowsForConflicts());

        PreCommitCondition cellAndRowLockAcquired = ignored -> {
            LockResponse cellLockResponse = acquireCellLock(ROW, COLUMN);
            LockResponse rowLockResponse = acquireRowLock(ROW);

            assertThat(cellLockResponse.wasSuccessful()).isFalse();
            assertThat(rowLockResponse.wasSuccessful()).isFalse();
        };

        commitWriteWith(cellAndRowLockAcquired, conflictHandler);
    }

    @ParameterizedTest
    @MethodSource("conflictHandlers")
    public void canAcquireLockOnMultipleCellsOnSameRow(ConflictHandler conflictHandler) {
        Assumptions.assumeTrue(conflictHandler.lockCellsForConflicts());

        PreCommitCondition canAcquireLockOnDifferentCell = ignored -> {
            LockResponse response = acquireCellLock(ROW, OTHER_COLUMN);
            assertThat(response.wasSuccessful()).isTrue();
        };

        commitWriteWith(canAcquireLockOnDifferentCell, conflictHandler);
    }

    private void commitWriteWith(PreCommitCondition preCommitCondition, ConflictHandler conflictHandler) {
        try (TransactionManager transactionManager = createTransactionManager(conflictHandler)) {
            Transaction transaction = Iterables.getOnlyElement(
                    transactionManager.startTransactions(ImmutableList.of(preCommitCondition)));
            put(transaction, ROW, COLUMN, "100");
            transaction.commit();
        }
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

    private TransactionManager createTransactionManager(ConflictHandler conflictHandler) {
        TestTransactionManagerImpl transactionManager = new TestTransactionManagerImpl(
                MetricsManagers.createForTests(),
                keyValueService,
                inMemoryTimelockExtension,
                lockService,
                transactionService,
                TestConflictDetectionManagers.createWithStaticConflictDetection(
                        ImmutableMap.of(TEST_TABLE, conflictHandler)),
                sweepStrategyManager,
                timestampCache,
                MultiTableSweepQueueWriter.NO_OP,
                knowledge,
                deleteExecutor);
        transactionManager.overrideConflictHandlerForTable(TEST_TABLE, conflictHandler);
        return transactionManager;
    }
}
