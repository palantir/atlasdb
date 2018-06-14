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
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cleaner.NoOpCleaner;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.api.PreCommitCondition;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionReadSentinelBehavior;
import com.palantir.lock.impl.LegacyTimelockService;
import com.palantir.lock.v2.LockResponse;

public abstract class AbstractSerializableTransactionWithRowLockTest extends AbstractSerializableTransactionTest {
    @Override
    protected Transaction startTransactionWithPreCommitCondition(PreCommitCondition preCommitCondition) {
        ImmutableMap<TableReference, ConflictHandler> tablesToWriteWrite = ImmutableMap.of(
                TEST_TABLE,
                ConflictHandler.SERIALIZABLE,
                TransactionConstants.TRANSACTION_TABLE,
                ConflictHandler.IGNORE_ALL);
        return new SerializableTransaction(
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
                AbstractTransactionTest.DELETE_EXECUTOR) {
            @Override
            protected Map<Cell, byte[]> transformGetsForTesting(Map<Cell, byte[]> map) {
                return Maps.transformValues(map, input -> input.clone());
            }
        };
    }

    @Test
    public void testSerializableDoesAcquireRowLock() {
        Transaction t0 = startTransaction();
        put(t0, "row1", "col1", "100");
        put(t0, "row1", "col2", "101");
        put(t0, "row2", "col1", "102");
        t0.commit();

        Transaction t1 = startTransactionWithPreCommitCondition((ignored) -> {
            LockResponse response = getRowLock("row1");
            assertFalse(response.wasSuccessful());
        });
        put(t1, "row1", "col2", "102");
        t1.commit();
    }

    @Test
    public void testSerializableDoesNotAcquireCellLock() {
        Transaction t0 = startTransaction();
        put(t0, "row1", "col1", "100");
        put(t0, "row1", "col2", "101");
        put(t0, "row2", "col1", "102");
        t0.commit();

        Transaction t1 = startTransactionWithPreCommitCondition((ignored) -> {
            LockResponse response = getCellLock("row1", "col2");
            //current lock implementation allows you to get a cell lock on a row that is already locked
            assertTrue(response.wasSuccessful());
        });
        put(t1, "row1", "col2", "102");
        t1.commit();
    }
}
