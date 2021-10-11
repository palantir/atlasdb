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
package com.palantir.atlasdb.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.AtlasDbTestCase;
import com.palantir.atlasdb.cache.DefaultTimestampCache;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.sweep.queue.MultiTableSweepQueueWriter;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManager;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManagers;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManagers;
import com.palantir.atlasdb.transaction.impl.TestTransactionManagerImpl;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.common.base.AbortingVisitor;
import com.palantir.common.base.AbortingVisitors;
import com.palantir.common.base.BatchingVisitable;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.common.exception.TableMappingNotFoundException;
import java.util.Map;
import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.Test;

public class TableMigratorTest extends AtlasDbTestCase {
    @Test
    public void testNeedArguments() {
        TableMigratorBuilder builder = new TableMigratorBuilder();
        assertThatThrownBy(builder::build).isInstanceOf(Exception.class);
    }

    @SuppressWarnings({"checkstyle:Indentation", "checkstyle:RightCurly"}) // Table/IndexDefinition syntax
    @Test
    public void testMigrationToDifferentKvs() throws TableMappingNotFoundException {
        final TableReference tableRef = TableReference.create(Namespace.DEFAULT_NAMESPACE, "table");
        final TableReference namespacedTableRef =
                TableReference.createFromFullyQualifiedName("namespace." + tableRef.getTablename());
        TableDefinition definition = new TableDefinition() {
            {
                rowName();
                rowComponent("r", ValueType.BLOB);
                columns();
                column("c", "c", ValueType.BLOB);
            }
        };
        keyValueService.createTable(tableRef, definition.toTableMetadata().persistToBytes());
        keyValueService.createTable(
                namespacedTableRef, definition.toTableMetadata().persistToBytes());
        keyValueService.putMetadataForTable(
                namespacedTableRef, definition.toTableMetadata().persistToBytes());

        final Cell theCell = Cell.create(PtBytes.toBytes("r1"), PtBytes.toBytes("c"));
        final byte[] theValue = PtBytes.toBytes("v1");
        txManager.runTaskWithRetry((TransactionTask<Void, RuntimeException>) txn -> {
            Map<Cell, byte[]> values = ImmutableMap.of(theCell, theValue);
            txn.put(tableRef, values);
            txn.put(namespacedTableRef, values);
            return null;
        });

        final InMemoryKeyValueService kvs2 = new InMemoryKeyValueService(false);
        final ConflictDetectionManager cdm2 = ConflictDetectionManagers.createWithNoConflictDetection();
        final SweepStrategyManager ssm2 = SweepStrategyManagers.completelyConservative();
        final MetricsManager metricsManager = MetricsManagers.createForTests();
        final TestTransactionManagerImpl txManager2 = new TestTransactionManagerImpl(
                metricsManager,
                kvs2,
                timelockServices,
                lockService,
                transactionService,
                cdm2,
                ssm2,
                DefaultTimestampCache.createForTests(),
                MultiTableSweepQueueWriter.NO_OP,
                MoreExecutors.newDirectExecutorService());
        kvs2.createTable(tableRef, definition.toTableMetadata().persistToBytes());
        kvs2.createTable(namespacedTableRef, definition.toTableMetadata().persistToBytes());

        TableReference checkpointTable = TableReference.create(Namespace.DEFAULT_NAMESPACE, "checkpoint");
        GeneralTaskCheckpointer checkpointer = new GeneralTaskCheckpointer(checkpointTable, kvs2, txManager2);

        for (final TableReference name : Lists.newArrayList(tableRef, namespacedTableRef)) {
            TransactionRangeMigrator rangeMigrator = new TransactionRangeMigratorBuilder()
                    .srcTable(name)
                    .readTxManager(txManager)
                    .txManager(txManager2)
                    .checkpointer(checkpointer)
                    .build();
            TableMigratorBuilder builder = new TableMigratorBuilder()
                    .srcTable(name)
                    .partitions(1)
                    .executor(PTExecutors.newSingleThreadExecutor())
                    .checkpointer(checkpointer)
                    .rangeMigrator(rangeMigrator);
            TableMigrator migrator = builder.build();
            migrator.migrate();
        }
        checkpointer.deleteCheckpoints();

        final ConflictDetectionManager verifyCdm = ConflictDetectionManagers.createWithNoConflictDetection();
        final SweepStrategyManager verifySsm = SweepStrategyManagers.completelyConservative();
        final TestTransactionManagerImpl verifyTxManager = new TestTransactionManagerImpl(
                metricsManager,
                kvs2,
                timelockServices,
                lockService,
                transactionService,
                verifyCdm,
                verifySsm,
                DefaultTimestampCache.createForTests(),
                MultiTableSweepQueueWriter.NO_OP,
                MoreExecutors.newDirectExecutorService());
        final MutableLong count = new MutableLong();
        for (final TableReference name : Lists.newArrayList(tableRef, namespacedTableRef)) {
            verifyTxManager.runTaskReadOnly((TransactionTask<Void, RuntimeException>) txn -> {
                BatchingVisitable<RowResult<byte[]>> bv = txn.getRange(name, RangeRequest.all());
                bv.batchAccept(
                        1000, AbortingVisitors.batching(new AbortingVisitor<RowResult<byte[]>, RuntimeException>() {
                            @Override
                            public boolean visit(RowResult<byte[]> item) throws RuntimeException {
                                Iterable<Map.Entry<Cell, byte[]>> cells = item.getCells();
                                Map.Entry<Cell, byte[]> entry = Iterables.getOnlyElement(cells);
                                assertThat(entry.getKey()).isEqualTo(theCell);
                                assertThat(entry.getValue()).isEqualTo(theValue);
                                count.increment();
                                return true;
                            }
                        }));
                return null;
            });
        }
        assertThat(count.longValue()).isEqualTo(2L);
    }
}
