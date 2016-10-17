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

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
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
import java.util.stream.Collectors;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.config.LockLeader;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.AbstractAtlasDbKeyValueServiceTest;
import com.palantir.atlasdb.keyvalue.impl.TableSplittingKeyValueService;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;

public class CassandraKeyValueServiceIntegrationTest extends AbstractAtlasDbKeyValueServiceTest {
    private static final long LOCK_ID = 123456789;

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
        return CassandraKeyValueService.create(
                CassandraKeyValueServiceConfigManager.createSimpleManager(CassandraTestSuite.cassandraKvsConfig),
                CassandraTestSuite.leaderConfig,
                logger);
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
            throw new IllegalArgumentException("Can't run this cassandra-specific test against a non-cassandra KVS");
        }

        kvs.createTable(testTable, tableMetadata);

        List<CfDef> knownCfs = kvs.clientPool.runWithRetry(client ->
                client.describe_keyspace("atlasdb").getCf_defs());
        CfDef clusterSideCf = Iterables.getOnlyElement(knownCfs.stream()
                .filter(cf -> cf.getName().equals("ns__never_seen"))
                .collect(Collectors.toList()));

        assertTrue("After serialization and deserialization to database, Cf metadata did not match.",
                CassandraKeyValueServices.isMatchingCf(kvs.getCfForTable(testTable, tableMetadata), clusterSideCf));
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
        CassandraKeyValueService ckvs = (CassandraKeyValueService) keyValueService;
        UniqueSchemaMutationLockTable lockTable = new UniqueSchemaMutationLockTable(
                new SchemaMutationLockTables(ckvs.clientPool, CassandraTestSuite.cassandraKvsConfig),
                LockLeader.I_AM_THE_LOCK_LEADER);
        SchemaMutationLockTestTools lockTestTools = new SchemaMutationLockTestTools(ckvs.clientPool, lockTable);

        // grab lock
        lockTestTools.setLocksTableValue(LOCK_ID, 0);

        ckvs.cleanUpSchemaMutationLockTablesState();

        CqlResult result = lockTestTools.readLocksTable();
        assertThat(result.getRows(), is(empty()));
    }

    private static int getAmountOfGarbageInMetadataTable(KeyValueService keyValueService, TableReference tableRef) {
        return keyValueService.getAllTimestamps(
                AtlasDbConstants.METADATA_TABLE,
                ImmutableSet.of(Cell.create(
                        tableRef.getQualifiedName().getBytes(StandardCharsets.UTF_8),
                        "m".getBytes(StandardCharsets.UTF_8))),
                Long.MAX_VALUE).size();
    }
}
