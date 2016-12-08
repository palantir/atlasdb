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
package com.palantir.atlasdb.schema;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.mutable.MutableLong;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.AtlasDbTestCase;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.TableMappingService;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.NamespaceMappingKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.StaticTableMappingService;
import com.palantir.atlasdb.keyvalue.impl.TableRemappingKeyValueService;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManager;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManagers;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManagers;
import com.palantir.atlasdb.transaction.impl.TestTransactionManagerImpl;
import com.palantir.common.base.AbortingVisitor;
import com.palantir.common.base.AbortingVisitors;
import com.palantir.common.base.BatchingVisitable;
import com.palantir.common.concurrent.PTExecutors;

public class TableMigratorTest extends AtlasDbTestCase {
    @Test
    public void testNeedArguments() {
        TableMigratorBuilder builder = new TableMigratorBuilder();
        try {
            builder.build();
            Assert.fail();
        } catch (Exception e) {
            // expected
        }
    }

    @Test
    public void testMigrationToDifferentKvs() {
        final TableReference tableRef = TableReference.create(Namespace.DEFAULT_NAMESPACE, "table");
        final TableReference namespacedTableRef = TableReference.createFromFullyQualifiedName("namespace." + tableRef.getTablename());
        TableDefinition definition = new TableDefinition() {{
                rowName();
                rowComponent("r", ValueType.BLOB);
            columns();
                column("c", "c", ValueType.BLOB);
        }};
        keyValueService.createTable(tableRef, definition.toTableMetadata().persistToBytes());
        keyValueService.createTable(namespacedTableRef, definition.toTableMetadata().persistToBytes());
        keyValueService.putMetadataForTable(namespacedTableRef, definition.toTableMetadata().persistToBytes());

        TableMappingService tableMap = StaticTableMappingService.create(keyValueService);
        final TableReference shortTableRef = tableMap.getMappedTableName(namespacedTableRef);

        final Cell theCell = Cell.create(PtBytes.toBytes("r1"), PtBytes.toBytes("c"));
        final byte[] theValue = PtBytes.toBytes("v1");
        txManager.runTaskWithRetry(new TransactionTask<Void, RuntimeException>() {
            @Override
            public Void execute(Transaction t) {
                Map<Cell, byte[]> values = ImmutableMap.of(
                        theCell,
                        theValue);
                t.put(tableRef, values);
                t.put(namespacedTableRef, values);
                return null;
            }
        });

        // migration doesn't use namespace mapping
        final InMemoryKeyValueService kvs2 = new InMemoryKeyValueService(false);
        final ConflictDetectionManager cdm2 = ConflictDetectionManagers.withoutConflictDetection(kvs2);
        final SweepStrategyManager ssm2 = SweepStrategyManagers.completelyConservative(kvs2);
        final TestTransactionManagerImpl txManager2 = new TestTransactionManagerImpl(
                kvs2,
                timestampService,
                lockClient,
                lockService,
                transactionService,
                cdm2,
                ssm2);
        kvs2.createTable(tableRef, definition.toTableMetadata().persistToBytes());
        kvs2.createTable(shortTableRef, definition.toTableMetadata().persistToBytes());

        GeneralTaskCheckpointer checkpointer = new GeneralTaskCheckpointer(TableReference.create(Namespace.DEFAULT_NAMESPACE, "checkpoint"), kvs2, txManager2);
        // The namespaced table is migrated under the short name.
        for (final TableReference name : Lists.newArrayList(tableRef, shortTableRef)) {
            TransactionRangeMigrator rangeMigrator = new TransactionRangeMigratorBuilder().
                    srcTable(name).
                    readTxManager(txManager).
                    txManager(txManager2).
                    checkpointer(checkpointer).
                    build();
            TableMigratorBuilder builder = new TableMigratorBuilder().
                    srcTable(name).
                    partitions(1).
                    executor(PTExecutors.newSingleThreadExecutor()).
                    checkpointer(checkpointer).
                    rangeMigrator(rangeMigrator);
            TableMigrator migrator = builder.build();
            migrator.migrate();
        }
        checkpointer.deleteCheckpoints();

        final KeyValueService verifyKvs = NamespaceMappingKeyValueService.create(TableRemappingKeyValueService.create(kvs2, tableMap));
        final ConflictDetectionManager verifyCdm = ConflictDetectionManagers.withoutConflictDetection(verifyKvs);
        final SweepStrategyManager verifySsm = SweepStrategyManagers.completelyConservative(verifyKvs);
        final TestTransactionManagerImpl verifyTxManager = new TestTransactionManagerImpl(
                verifyKvs,
                timestampService,
                lockClient,
                lockService,
                transactionService,
                verifyCdm,
                verifySsm);
        final MutableLong count = new MutableLong();
        for (final TableReference name : Lists.newArrayList(tableRef, namespacedTableRef)) {
            verifyTxManager.runTaskReadOnly(new TransactionTask<Void, RuntimeException>() {
                @Override
                public Void execute(Transaction t) {
                    BatchingVisitable<RowResult<byte[]>> bv = t.getRange(name, RangeRequest.all());
                    bv.batchAccept(1000, AbortingVisitors.batching(new AbortingVisitor<RowResult<byte[]>, RuntimeException>() {
                        @Override
                        public boolean visit(RowResult<byte[]> item) {
                            Iterable<Entry<Cell, byte[]>> cells = item.getCells();
                            Entry<Cell, byte[]> e = Iterables.getOnlyElement(cells);
                            Assert.assertEquals(theCell, e.getKey());
                            Assert.assertArrayEquals(theValue, e.getValue());
                            count.increment();
                            return true;
                        }
                    }));
                    return null;
                }
            });
        }
        Assert.assertEquals(2L, count.longValue());
    }
}
