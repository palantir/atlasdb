/**
 * Copyright 2016 Palantir Technologies
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

package com.palantir.atlasdb.keyvalue.cassandra;

import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.config.LockLeader;
import com.palantir.atlasdb.containers.CassandraContainer;
import com.palantir.atlasdb.containers.Containers;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.AbstractKeyValueServiceTest;
import com.palantir.atlasdb.keyvalue.impl.TableSplittingKeyValueService;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;

@RunWith(Parameterized.class)
public class CassandraKeyValueServiceIntegrationTest extends AbstractKeyValueServiceTest {
    private static final long LOCK_ID = 123456789;

    @Parameterized.Parameters
    public static Iterable<?> keyValueServicesToTest() {
        return CassandraContainer.testWithBothThriftAndCql();
    }

    @Parameterized.Parameter
    public Supplier<KeyValueService> kvs;

    @ClassRule
    public static final Containers CONTAINERS = new Containers(CassandraKeyValueServiceIntegrationTest.class)
            .with(new CassandraContainer());

    private KeyValueService keyValueService;
    private ExecutorService executorService;
    private Logger logger = mock(Logger.class);

    private TableReference testTable = TableReference.createFromFullyQualifiedName("ns.never_seen");
    private byte[] tableMetadata = new TableDefinition() {
        {
            rowName();
            rowComponent("blob", ValueType.BLOB);
            columns();
            column("boblawblowlawblob", "col", ValueType.BLOB);
            conflictHandler(ConflictHandler.IGNORE_ALL);
            sweepStrategy(TableMetadataPersistence.SweepStrategy.NOTHING);
            explicitCompressionBlockSizeKB(8);
            rangeScanAllowed();
            ignoreHotspottingChecks();
            negativeLookups();
            cachePriority(TableMetadataPersistence.CachePriority.COLD);
        }
    }.toTableMetadata().persistToBytes();

    @Before
    public void setupKvs() {
        keyValueService = getKeyValueService();
        executorService = Executors.newFixedThreadPool(4);
    }

    @After
    public void cleanUp() {
        executorService.shutdown();
    }

    @Override
    protected KeyValueService getKeyValueService() {
        return kvs.get();
    }

    @Override
    protected boolean reverseRangesSupported() {
        return false;
    }

    @Override
    @Ignore
    public void testGetAllTableNames() {
        //
    }

    @Test
    public void testCreateTableCaseInsensitive() throws TException {
        TableReference table1 = TableReference.createFromFullyQualifiedName("ns.tAbLe");
        TableReference table2 = TableReference.createFromFullyQualifiedName("ns.table");
        TableReference table3 = TableReference.createFromFullyQualifiedName("ns.TABle");

        keyValueService.dropTable(table1);
        keyValueService.dropTable(table2);
        keyValueService.dropTable(table3);

        keyValueService.createTable(table1, AtlasDbConstants.GENERIC_TABLE_METADATA);
        keyValueService.createTable(table2, AtlasDbConstants.GENERIC_TABLE_METADATA);
        keyValueService.createTable(table3, AtlasDbConstants.GENERIC_TABLE_METADATA);
        Set<TableReference> allTables = keyValueService.getAllTableNames();
        Preconditions.checkArgument(allTables.contains(table1));
        Preconditions.checkArgument(!allTables.contains(table2));
        Preconditions.checkArgument(!allTables.contains(table3));
    }

    @Test
    public void testCfEqualityChecker() throws TException {
        CassandraKeyValueService kvs;
        if (keyValueService instanceof CassandraKeyValueService) {
            kvs = (CassandraKeyValueService) keyValueService;
        } else if (keyValueService instanceof TableSplittingKeyValueService) { // scylla tests
            KeyValueService delegate = ((TableSplittingKeyValueService) keyValueService).getDelegate(testTable);
            assertTrue("The nesting of Key Value Services has apparently changed",
                    delegate instanceof CassandraKeyValueService);
            kvs = (CassandraKeyValueService) delegate;
        } else {
            return; // this test tests functionality specific to C*KVS
        }

        kvs.createTable(testTable, tableMetadata);

        List<CfDef> knownCfs = kvs.clientPool.runWithRetry(client ->
                client.describe_keyspace("atlasdb").getCf_defs());
        CfDef clusterSideCf = Iterables.getOnlyElement(knownCfs.stream()
                .filter(cf -> cf.getName().equals("ns__never_seen"))
                .collect(Collectors.toList()));

        assertTrue("After serialization and deserialization to database, Cf metadata did not match.",
                ColumnFamilyDefinitions.isMatchingCf(kvs.getCfForTable(testTable, tableMetadata), clusterSideCf));
    }

    @Test
    public void shouldNotErrorForTimestampTableWhenCreatingCassandraKvs() throws Exception {
        verify(logger, never()).error(startsWith("Found a table " + AtlasDbConstants.TIMESTAMP_TABLE));
    }

    @Test
    public void repeatedDropTableDoesNotAccumulateGarbage() {
        int preExistingGarbageBeforeTest = getAmountOfGarbageInMetadataTable(keyValueService, testTable);

        for (int i = 0; i < 3; i++) {
            keyValueService.createTable(testTable, tableMetadata);
            keyValueService.dropTable(testTable);
        }

        int garbageAfterTest = getAmountOfGarbageInMetadataTable(keyValueService, testTable);

        assertThat(garbageAfterTest, lessThanOrEqualTo(preExistingGarbageBeforeTest));
    }

    @Test
    public void testLockTablesStateCleanUp() throws Exception {
        // we can ignore this on CQL KVS because we don't use this style of locking in it
        if (keyValueService instanceof CassandraKeyValueService) {
            CassandraKeyValueService ckvs = (CassandraKeyValueService) keyValueService;
            SchemaMutationLockTables lockTables = new SchemaMutationLockTables(
                    ckvs.clientPool,
                    CassandraContainer.THRIFT_CONFIG);
            SchemaMutationLockTestTools lockTestTools = new SchemaMutationLockTestTools(
                    ckvs.clientPool,
                    new UniqueSchemaMutationLockTable(lockTables, LockLeader.I_AM_THE_LOCK_LEADER));

            grabLock(lockTestTools);
            createExtraLocksTable(lockTables);

            ckvs.cleanUpSchemaMutationLockTablesState();

            // depending on which table we pick when running cleanup on multiple lock tables, we might have a table with
            // no rows or a table with a single row containing the cleared lock value (both are valid clean states).
            List<CqlRow> resultRows = lockTestTools.readLocksTable().getRows();
            assertThat(resultRows, either(is(empty())).or(hasSize(1)));
            if (resultRows.size() == 1) {
                Column resultColumn = Iterables.getOnlyElement(Iterables.getOnlyElement(resultRows).getColumns());
                long lockId = SchemaMutationLock.getLockIdFromColumn(resultColumn);
                assertThat(lockId, is(SchemaMutationLock.GLOBAL_DDL_LOCK_CLEARED_ID));
            }
        }
    }

    private void grabLock(SchemaMutationLockTestTools lockTestTools) throws TException {
        lockTestTools.setLocksTableValue(LOCK_ID, 0);
    }

    private void createExtraLocksTable(SchemaMutationLockTables lockTables) throws TException {
        TableReference originalTable = Iterables.getOnlyElement(lockTables.getAllLockTables());
        lockTables.createLockTable();
        assertThat(lockTables.getAllLockTables(), hasItem(not(originalTable)));
    }

    private static int getAmountOfGarbageInMetadataTable(KeyValueService keyValueService, TableReference tableRef) {
        return keyValueService.getAllTimestamps(
                AtlasDbConstants.DEFAULT_METADATA_TABLE,
                ImmutableSet.of(Cell.create(
                        tableRef.getQualifiedName().getBytes(StandardCharsets.UTF_8),
                        "m".getBytes(StandardCharsets.UTF_8))),
                Long.MAX_VALUE).size();
    }
}
