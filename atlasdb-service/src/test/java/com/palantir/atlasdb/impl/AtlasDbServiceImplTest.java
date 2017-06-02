/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.impl;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.palantir.atlasdb.cleaner.Cleaner;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ClusterAvailabilityStatus;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManager;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManager;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRequest;
import com.palantir.lock.RemoteLockService;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.impl.LockServiceImpl;
import com.palantir.timestamp.InMemoryTimestampService;
import com.palantir.timestamp.TimestampService;

public class AtlasDbServiceImplTest {
    public static final TableReference FROM_FULLY_QUALIFIED_NAME = TableReference.createFromFullyQualifiedName("Test.Test");
    private KeyValueService kvs;
    private AtlasDbServiceImpl atlasDbService;

    @Before
    public void setUp() {
        kvs = mock(KeyValueService.class);
        SerializableTransactionManager txManager = mock(SerializableTransactionManager.class);
        TableMetadataCache metadataCache = mock(TableMetadataCache.class);
        atlasDbService = new AtlasDbServiceImpl(kvs, txManager, metadataCache);
    }

    @Test
    public void shouldTruncateSystemTables() throws Exception {
        atlasDbService.truncateTable("_locks");

        TableReference tableToTruncate = TableReference.createWithEmptyNamespace("_locks");
        verify(kvs, atLeastOnce()).truncateTable(tableToTruncate);
    }

    @Test
    public void shouldTruncateNamespacedTables() throws Exception {
        atlasDbService.truncateTable("ns.table");

        TableReference tableToTruncate = TableReference.createFromFullyQualifiedName("ns.table");
        verify(kvs, atLeastOnce()).truncateTable(tableToTruncate);
    }

    @Test
    public void attempt() throws Exception {
        KeyValueService newKvs = mock(KeyValueService.class);
        when(newKvs.getClusterAvailabilityStatus()).thenReturn(ClusterAvailabilityStatus.NO_QUORUM_AVAILABLE);
        Cell cell = Cell.create(PtBytes.toBytes("row1"), PtBytes.toBytes("column1"));
        doThrow(new RuntimeException("Got you")).when(newKvs).put(
                FROM_FULLY_QUALIFIED_NAME, ImmutableMap.of(cell, PtBytes.toBytes("value")), 1);

        TimestampService timestampService = new InMemoryTimestampService();
        LockClient lockClient = new LockClient("HA:");
        RemoteLockService lockService = LockServiceImpl.create();
        TransactionService transactionService = mock(TransactionService.class);
        Supplier<AtlasDbConstraintCheckingMode> constraintModeSupplier = () -> AtlasDbConstraintCheckingMode.NO_CONSTRAINT_CHECKING;
        ConflictDetectionManager conflictDetectionManager = mock(ConflictDetectionManager.class);
        when(conflictDetectionManager.get(FROM_FULLY_QUALIFIED_NAME)).thenReturn(ConflictHandler.IGNORE_ALL);
        SweepStrategyManager sweepStrategyManager = mock(SweepStrategyManager.class);
        Cleaner cleaner = mock(Cleaner.class);

        SerializableTransactionManager txManager = new SerializableTransactionManager(newKvs, timestampService, lockClient, lockService, transactionService, constraintModeSupplier, conflictDetectionManager, sweepStrategyManager, cleaner);
        LockDescriptor descriptor = StringLockDescriptor.of("lock");
        LockRequest request = LockRequest.builder(ImmutableSortedMap.of(descriptor, LockMode.WRITE))
                .doNotBlock()
                .build();
        HeldLocksToken expiredLockToken = lockService.lockAndGetHeldLocks("bla", request);

        txManager.runTaskWithLocksWithRetry(ImmutableList.of(expiredLockToken), () -> null, (tx, locks) -> {tx.put(TableReference.createFromFullyQualifiedName("Test.Test"), ImmutableMap.of(cell, PtBytes.toBytes("value")));
            return null;
        }, false);

//        Transaction transaction = new SnapshotTransaction()
//        txManager.finishRunTaskWithLockThrowOnConflict()
    }
}
