/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.Optional;

import org.junit.Test;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cleaner.NoOpCleaner;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.api.PreCommitCondition;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionReadSentinelBehavior;
import com.palantir.atlasdb.transaction.impl.logging.CommitProfileProcessor;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.lock.impl.LegacyTimelockService;
import com.palantir.lock.v2.LockResponse;

public abstract class AbstractSerializableTransactionWithCellLockTest extends AbstractSerializableTransactionTest {
    @Override
    protected Transaction startTransaction() {
        return startTransactionWithPreCommitCondition(PreCommitConditions.NO_OP);
    }

    private Transaction startTransactionWithPreCommitCondition(PreCommitCondition preCommitCondition) {
        ImmutableMap<TableReference, ConflictHandler> tablesToWriteWrite = ImmutableMap.of(
                TEST_TABLE,
                ConflictHandler.SERIALIZABLE_CELL,
                TransactionConstants.TRANSACTION_TABLE,
                ConflictHandler.IGNORE_ALL);
        return new SerializableTransaction(
                MetricsManagers.createForTests(),
                keyValueService,
                new LegacyTimelockService(timestampService, lockService, lockClient),
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
                AtlasDbConstants.DEFAULT_TRANSACTION_LOCK_ACQUIRE_TIMEOUT_MS,
                AbstractTransactionTest.GET_RANGES_EXECUTOR,
                AbstractTransactionTest.DEFAULT_GET_RANGES_CONCURRENCY,
                getSweepQueueWriterInitialized(),
                MoreExecutors.newDirectExecutorService(),
                CommitProfileProcessor.createNonLogging(metricsManager)) {
            @Override
            protected Map<Cell, byte[]> transformGetsForTesting(Map<Cell, byte[]> map) {
                return Maps.transformValues(map, input -> input.clone());
            }
        };
    }

    @Test
    public void testCommitDoesNotAcquireRowLock() {
        Transaction transaction = startTransactionWithPreCommitCondition((ignored) -> {
            LockResponse response = acquireRowLock("row1");
            assertTrue(response.wasSuccessful());
        });
        put(transaction, "row1", "col2", "102");
        transaction.commit();
    }

    @Test
    public void testCommitAcquiresCellLock() {
        Transaction transaction = startTransactionWithPreCommitCondition((ignored) -> {
            LockResponse response = acquireCellLock("row1", "col2");
            //current lock implementation allows you to get a cell lock on a row that is already locked
            assertFalse(response.wasSuccessful());
        });
        put(transaction, "row1", "col2", "102");
        transaction.commit();
    }

    @Test
    public void testCanAcquireLockOnMultipleCellsOnSameRow() {
        Transaction transaction = startTransactionWithPreCommitCondition((ignored) -> {
            LockResponse response = acquireCellLock("row1", "col1");
            assertTrue(response.wasSuccessful());
        });
        put(transaction, "row1", "col2", "102");
        transaction.commit();
    }
}
