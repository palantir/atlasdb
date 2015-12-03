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
package com.palantir.atlasdb.keyvalue.impl;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cleaner.Cleaner;
import com.palantir.atlasdb.cleaner.NoOpCleaner;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.table.common.TableTasks;
import com.palantir.atlasdb.table.common.TableTasks.DiffStats;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionManager;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManager;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManagers;
import com.palantir.atlasdb.transaction.impl.SnapshotTransactionManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManagers;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.transaction.service.TransactionServices;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.impl.LockServiceImpl;
import com.palantir.timestamp.InMemoryTimestampService;
import com.palantir.timestamp.TimestampService;

public class TableTasksTest {
    private KeyValueService kvs;
    private LockServiceImpl lockService;
    private LockAwareTransactionManager txManager;

    @Before
    @SuppressWarnings("serial")
    public void setup() {
        kvs = new InMemoryKeyValueService(true);
        TimestampService tsService = new InMemoryTimestampService();
        LockClient lockClient = LockClient.of("sweep client");
        lockService = LockServiceImpl.create(new LockServerOptions() {
            @Override
            public boolean isStandaloneServer() {
                return false;
            }
        });
        TransactionService txService = TransactionServices.createTransactionService(kvs);
        Supplier<AtlasDbConstraintCheckingMode> constraints = Suppliers.ofInstance(AtlasDbConstraintCheckingMode.NO_CONSTRAINT_CHECKING);
        ConflictDetectionManager cdm = ConflictDetectionManagers.createDefault(kvs);
        SweepStrategyManager ssm = SweepStrategyManagers.createDefault(kvs);
        Cleaner cleaner = new NoOpCleaner();
        SnapshotTransactionManager snapshotTransactionManager = new SnapshotTransactionManager(kvs, tsService, lockClient, lockService, txService, constraints, cdm, ssm, cleaner, false);
        txManager = snapshotTransactionManager;
    }

    @After
    public void teardown() {
        lockService.close();
        kvs.close();
    }

    @Test
    public void testDiffTask() throws InterruptedException {
        Random rand = new Random();
        kvs.createTable("table1", AtlasDbConstants.EMPTY_TABLE_METADATA);
        kvs.createTable("table2", AtlasDbConstants.EMPTY_TABLE_METADATA);
        HashMultimap<Integer, Integer> keys1 = HashMultimap.create();
        HashMultimap<Integer, Integer> keys2 = HashMultimap.create();
        int key = 0;
        for (int col = 0; col < 256; col++) {
            int r = rand.nextInt(3);
            if (r >= 1) {
                keys1.put(key, col);
                kvs.put("table1", ImmutableMap.of(Cell.create(new byte[]{(byte) key}, new byte[]{(byte) col}), new byte[] {0}), 1);
            }
            if (r <= 1) {
                keys2.put(key, col);
                kvs.put("table2", ImmutableMap.of(Cell.create(new byte[]{(byte) key}, new byte[]{(byte) col}), new byte[] {0}), 1);
            }
            if (rand.nextBoolean()) {
                key++;
            }
        }
        TransactionServices.createTransactionService(kvs).putUnlessExists(1, 1);
        AtomicLong rowsOnlyInSource = new AtomicLong();
        AtomicLong rowsPartiallyInCommon = new AtomicLong();
        AtomicLong rowsCompletelyInCommon = new AtomicLong();
        AtomicLong cellsOnlyInSource = new AtomicLong();
        AtomicLong cellsInCommon = new AtomicLong();
        DiffStats stats = new TableTasks.DiffStats(rowsOnlyInSource, rowsPartiallyInCommon, rowsCompletelyInCommon, cellsOnlyInSource, cellsInCommon);
        TableTasks.diff(txManager, MoreExecutors.newDirectExecutorService(), "table1", "table2", 10, 1, stats, new TableTasks.DiffVisitor() {
            @Override
            public void visit(Transaction t, Iterator<Cell> partialDiff) {
                Iterators.size(partialDiff);
            }
        });
        long sourceOnlyCells = 0, commonCells = 0;
        for (Entry<Integer, Integer> cell : keys1.entries()) {
            if (keys2.containsEntry(cell.getKey(), cell.getValue())) {
                commonCells++;
            } else {
                sourceOnlyCells++;
            }
        }
        long disjointRows = 0, partialRows = 0, commonRows = 0;
        for (int k : keys1.keySet()) {
            if (Collections.disjoint(keys2.get(k), keys1.get(k))) {
                disjointRows++;
            } else if (keys2.get(k).containsAll(keys1.get(k))) {
                commonRows++;
            } else {
                partialRows++;
            }
        }

        Assert.assertEquals(commonCells, cellsInCommon.get());
        Assert.assertEquals(sourceOnlyCells, cellsOnlyInSource.get());
        Assert.assertEquals(disjointRows, rowsOnlyInSource.get());
        Assert.assertEquals(commonRows, rowsCompletelyInCommon.get());
        Assert.assertEquals(partialRows, rowsPartiallyInCommon.get());
    }
}
