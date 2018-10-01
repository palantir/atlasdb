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

package com.palantir.atlasdb.keyvalue.cassandra;

import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.thrift.TException;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.io.BaseEncoding;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cli.command.CleanCassLocksStateCommand;
import com.palantir.atlasdb.config.LockLeader;
import com.palantir.atlasdb.containers.CassandraContainer;
import com.palantir.atlasdb.containers.Containers;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.AbstractKeyValueServiceTest;
import com.palantir.atlasdb.keyvalue.impl.TableSplittingKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.TracingPrefsConfig;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.table.description.ColumnMetadataDescription;
import com.palantir.atlasdb.table.description.NameMetadataDescription;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;

public class CassandraKeyValueServiceIntegrationTest extends AbstractKeyValueServiceTest {
    private static final CassandraContainer container = new CassandraContainer();

    @ClassRule
    public static final Containers CONTAINERS = new Containers(CassandraKeyValueServiceIntegrationTest.class)
            .with(container);

    private final Logger logger = mock(Logger.class);

    private final MetricsManager metricsManager = MetricsManagers.createForTests();

    private TableReference testTable = TableReference.createFromFullyQualifiedName("ns.never_seen");

    private static final int FOUR_DAYS_IN_SECONDS = 4 * 24 * 60 * 60;
    private static final int ONE_HOUR_IN_SECONDS = 60 * 60;

    private static final long STARTING_ATLAS_TIMESTAMP = 10_000_000;

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

    // notably, this metadata is different from the default AtlasDbConstants.GENERIC_TABLE_METADATA
    // to make sure the tests are actually exercising the correct retrieval codepaths
    static byte[] originalMetadata = new TableMetadata(
            new NameMetadataDescription(),
            new ColumnMetadataDescription(),
            ConflictHandler.RETRY_ON_VALUE_CHANGED,
            TableMetadataPersistence.LogSafety.SAFE)
            .persistToBytes();

    public static final Cell CELL = Cell.create(PtBytes.toBytes("row"), PtBytes.toBytes("column"));

    @Override
    protected KeyValueService getKeyValueService() {
        return createKvs(getConfigWithGcGraceSeconds(FOUR_DAYS_IN_SECONDS), logger);
    }

    private CassandraKeyValueService createKvs(CassandraKeyValueServiceConfig config, Logger testLogger) {
        // Mutation provider is needed, because deletes/sentinels are to be written after writes
        return CassandraKeyValueServiceImpl.create(
                metricsManager,
                config,
                CassandraContainer.LEADER_CONFIG,
                CassandraTestTools.getMutationProviderWithStartingTimestamp(STARTING_ATLAS_TIMESTAMP),
                testLogger);
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
    public void testCreateTableCaseInsensitive() {
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
    @SuppressWarnings("Slf4jConstantLogMessage")
    public void testGcGraceSecondsUpgradeIsApplied() throws TException {
        Logger testLogger = mock(Logger.class);
        //nth startup
        CassandraKeyValueService kvs = createKvs(getConfigWithGcGraceSeconds(FOUR_DAYS_IN_SECONDS), testLogger);
        kvs.createTable(testTable, AtlasDbConstants.GENERIC_TABLE_METADATA);
        assertThatGcGraceSecondsIs(kvs, FOUR_DAYS_IN_SECONDS);
        kvs.close();

        CassandraKeyValueService kvs2 = createKvs(getConfigWithGcGraceSeconds(ONE_HOUR_IN_SECONDS), testLogger);
        assertThatGcGraceSecondsIs(kvs2, ONE_HOUR_IN_SECONDS);
        kvs2.close();
        //n+1th startup with different GC grace seconds - should upgrade
        verify(testLogger, times(1))
                .info(startsWith("New table-related settings were applied on startup!!"));

        CassandraKeyValueService kvs3 = createKvs(getConfigWithGcGraceSeconds(ONE_HOUR_IN_SECONDS), testLogger);
        assertThatGcGraceSecondsIs(kvs3, ONE_HOUR_IN_SECONDS);
        //startup with same gc grace seconds as previous one - no upgrade
        verify(testLogger, times(2))
                .info(startsWith("No tables are being upgraded on startup. No updated table-related settings found."));
        kvs3.close();
    }

    private void assertThatGcGraceSecondsIs(CassandraKeyValueService kvs, int gcGraceSeconds) throws TException {
        List<CfDef> knownCfs = kvs.getClientPool().runWithRetry(client ->
                client.describe_keyspace("atlasdb").getCf_defs());
        CfDef clusterSideCf = Iterables.getOnlyElement(knownCfs.stream()
                .filter(cf -> cf.getName().equals(getInternalTestTableName()))
                .collect(Collectors.toList()));
        assertThat(clusterSideCf.gc_grace_seconds, equalTo(gcGraceSeconds));
    }

    @Test
    public void testCfEqualityChecker() throws TException {
        CassandraKeyValueServiceImpl kvs;
        if (keyValueService instanceof CassandraKeyValueService) {
            kvs = (CassandraKeyValueServiceImpl) keyValueService;
        } else if (keyValueService instanceof TableSplittingKeyValueService) { // scylla tests
            KeyValueService delegate = ((TableSplittingKeyValueService) keyValueService).getDelegate(testTable);
            assertTrue("The nesting of Key Value Services has apparently changed",
                    delegate instanceof CassandraKeyValueService);
            kvs = (CassandraKeyValueServiceImpl) delegate;
        } else {
            throw new IllegalArgumentException("Can't run this cassandra-specific test against a non-cassandra KVS");
        }

        kvs.createTable(testTable, tableMetadata);

        List<CfDef> knownCfs = kvs.getClientPool().runWithRetry(client ->
                client.describe_keyspace("atlasdb").getCf_defs());
        CfDef clusterSideCf = Iterables.getOnlyElement(knownCfs.stream()
                .filter(cf -> cf.getName().equals(getInternalTestTableName()))
                .collect(Collectors.toList()));

        assertTrue("After serialization and deserialization to database, Cf metadata did not match.",
                ColumnFamilyDefinitions.isMatchingCf(kvs.getCfForTable(testTable, tableMetadata,
                        FOUR_DAYS_IN_SECONDS), clusterSideCf));
    }

    private ImmutableCassandraKeyValueServiceConfig getConfigWithGcGraceSeconds(int gcGraceSeconds) {
        return ImmutableCassandraKeyValueServiceConfig
                .copyOf(container.getConfig())
                .withGcGraceSeconds(gcGraceSeconds);
    }

    private String getInternalTestTableName() {
        return testTable.getQualifiedName().replaceFirst("\\.", "__");
    }

    @Test
    @SuppressWarnings("Slf4jConstantLogMessage")
    public void shouldNotErrorForTimestampTableWhenCreatingCassandraKvs() {
        verify(logger, never()).error(startsWith("Found a table {} that did not have persisted"), anyString());
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
        CassandraKeyValueServiceImpl ckvs = (CassandraKeyValueServiceImpl) keyValueService;
        SchemaMutationLockTables lockTables = new SchemaMutationLockTables(ckvs.getClientPool(), container.getConfig());
        SchemaMutationLockTestTools lockTestTools = new SchemaMutationLockTestTools(
                ckvs.getClientPool(),
                new UniqueSchemaMutationLockTable(lockTables, LockLeader.I_AM_THE_LOCK_LEADER));

        createExtraLocksTable(lockTables, ckvs);

        TracingQueryRunner queryRunner = new TracingQueryRunner(
                LoggerFactory.getLogger(CassandraKeyValueServiceIntegrationTest.class), new TracingPrefsConfig());
        CassandraSchemaLockCleaner cleaner = CassandraSchemaLockCleaner.create(container.getConfig(),
                ckvs.getClientPool(), lockTables, queryRunner);

        cleaner.cleanLocksState();

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

    @Test
    public void testCleanCassLocksStateCli() throws Exception {
        CassandraKeyValueServiceImpl ckvs = (CassandraKeyValueServiceImpl) keyValueService;
        SchemaMutationLockTables lockTables = new SchemaMutationLockTables(ckvs.getClientPool(), container.getConfig());
        SchemaMutationLockTestTools lockTestTools = new SchemaMutationLockTestTools(
                ckvs.getClientPool(),
                new UniqueSchemaMutationLockTable(lockTables, LockLeader.I_AM_THE_LOCK_LEADER));

        createExtraLocksTable(lockTables, ckvs);

        new CleanCassLocksStateCommand().runWithConfig(container.getConfig());

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

    @Test
    public void sweepSentinelsAreWrittenAtFreshTimestamp() throws Exception {
        TableReference tableReference =
                TableReference.createFromFullyQualifiedName("test." + RandomStringUtils.randomAlphanumeric(16));
        keyValueService.createTable(tableReference, AtlasDbConstants.GENERIC_TABLE_METADATA);

        keyValueService.addGarbageCollectionSentinelValues(tableReference, ImmutableList.of(CELL));

        putDummyValueAtCellAndTimestamp(
                tableReference,
                CELL,
                Value.INVALID_VALUE_TIMESTAMP,
                STARTING_ATLAS_TIMESTAMP - 1);

        Map<Cell, Value> results = keyValueService.get(tableReference, ImmutableMap.of(CELL, 1L));
        byte[] contents = results.get(CELL).getContents();
        assertThat(Arrays.equals(contents, PtBytes.EMPTY_BYTE_ARRAY), is(true));
    }

    @Test
    public void deletionTakesPlaceAtFreshTimestamp() throws Exception {
        TableReference tableReference =
                TableReference.createFromFullyQualifiedName("test." + RandomStringUtils.randomAlphanumeric(16));
        keyValueService.createTable(tableReference, AtlasDbConstants.GENERIC_TABLE_METADATA);
        byte[] data = PtBytes.toBytes("data");
        byte[] moreData = PtBytes.toBytes("data2");

        keyValueService.putWithTimestamps(tableReference, ImmutableListMultimap.of(CELL, Value.create(data, 8L)));
        keyValueService.putWithTimestamps(tableReference, ImmutableListMultimap.of(CELL, Value.create(moreData, 88L)));
        keyValueService.delete(tableReference, ImmutableListMultimap.of(CELL, 8L));

        putDummyValueAtCellAndTimestamp(tableReference, CELL, 8L, STARTING_ATLAS_TIMESTAMP - 1);
        Map<Cell, Value> results = keyValueService.get(tableReference, ImmutableMap.of(CELL, 8L + 1));
        assertThat(results.containsKey(CELL), is(false));
    }

    @Test
    public void rangeTombstonesWrittenAtFreshTimestamp() throws Exception {
        TableReference tableReference =
                TableReference.createFromFullyQualifiedName("test." + RandomStringUtils.randomAlphanumeric(16));
        keyValueService.createTable(tableReference, AtlasDbConstants.GENERIC_TABLE_METADATA);

        keyValueService.deleteAllTimestamps(
                tableReference,
                ImmutableMap.of(CELL, 1_234_567L),
                true);

        putDummyValueAtCellAndTimestamp(tableReference, CELL, 1337L, STARTING_ATLAS_TIMESTAMP - 1);
        Map<Cell, Value> resultExpectedCoveredByRangeTombstone =
                keyValueService.get(tableReference, ImmutableMap.of(CELL, 1337L + 1));
        assertThat(resultExpectedCoveredByRangeTombstone.containsKey(CELL), is(false));
    }

    @Test
    public void cassandraTimestampsAreNotUsedAsAtlasTimestampsForRangeTombstone() throws Exception {
        TableReference tableReference =
                TableReference.createFromFullyQualifiedName("test." + RandomStringUtils.randomAlphanumeric(16));
        keyValueService.createTable(tableReference, AtlasDbConstants.GENERIC_TABLE_METADATA);

        keyValueService.deleteAllTimestamps(
                tableReference,
                ImmutableMap.of(CELL, 1_234_567L),
                true);

        // A value written outside of the range tombstone should not be covered by the range tombstone, even if
        // the Cassandra timestamp of the value is much lower than that of the range tombstone.
        // This test is likely to fail if the implementation confuses Cassandra timestamps for Atlas timestamps.
        putDummyValueAtCellAndTimestamp(tableReference, CELL, 1_333_337L, STARTING_ATLAS_TIMESTAMP - 1);
        Map<Cell, Value> resultsOutsideRangeTombstone =
                keyValueService.get(tableReference, ImmutableMap.of(CELL, Long.MAX_VALUE));
        assertThat(resultsOutsideRangeTombstone.containsKey(CELL), is(true));
    }

    @Test
    public void oldMixedCaseMetadataStillVisible() {
        TableReference userTable = TableReference.createFromFullyQualifiedName("test.cAsEsEnSiTiVe");
        Cell oldMetadataCell = CassandraKeyValueServices.getOldMetadataCell(userTable);

        keyValueService.put(
                AtlasDbConstants.DEFAULT_METADATA_TABLE,
                ImmutableMap.of(oldMetadataCell, originalMetadata), System.currentTimeMillis());

        assertThat(
                Arrays.equals(keyValueService.getMetadataForTable(userTable), originalMetadata),
                is(true));
    }

    @Test
    public void metadataForNewTableIsLowerCased() {
        TableReference userTable = TableReference.createFromFullyQualifiedName("test.xXcOoLtAbLeNaMeXx");

        keyValueService.createTable(userTable, originalMetadata);

        assertThat(keyValueService.getMetadataForTables().keySet().stream()
                .anyMatch(tableRef -> tableRef.getQualifiedName().equals(userTable.getQualifiedName().toLowerCase())),
                is(true));
    }

    @Test
    public void metadataUpdateForExistingOldFormatMetadataUpdatesOldFormat() {
        TableReference userTable = TableReference.createFromFullyQualifiedName("test.tOoMaNyTeStS");
        Cell oldMetadataCell = CassandraKeyValueServices.getOldMetadataCell(userTable);

        byte[] tableMetadataUpdate = new TableMetadata(
                new NameMetadataDescription(),
                new ColumnMetadataDescription(),
                ConflictHandler.IGNORE_ALL, // <--- new, update that isn't in originalMetadata
                TableMetadataPersistence.LogSafety.SAFE)
                .persistToBytes();

        keyValueService.put(
                AtlasDbConstants.DEFAULT_METADATA_TABLE,
                ImmutableMap.of(oldMetadataCell, originalMetadata),
                System.currentTimeMillis());

        keyValueService.createTable(userTable, tableMetadataUpdate);

        assertThat(Arrays.equals(keyValueService.getMetadataForTable(userTable), tableMetadataUpdate), is(true));
    }

    @Test
    @SuppressWarnings("Slf4jConstantLogMessage")
    public void upgradeFromOlderInternalSchemaDoesNotErrorOnTablesWithUpperCaseCharacters() {
        TableReference tableRef = TableReference.createFromFullyQualifiedName("test.uPgrAdefRomolDerintErnalscHema");
        keyValueService.put(
                AtlasDbConstants.DEFAULT_METADATA_TABLE,
                ImmutableMap.of(CassandraKeyValueServices.getMetadataCell(tableRef), originalMetadata),
                System.currentTimeMillis());
        keyValueService.createTable(tableRef, originalMetadata);

        ((CassandraKeyValueServiceImpl) keyValueService).upgradeFromOlderInternalSchema();
        verify(logger, never()).error(anyString(), any(Object.class));
    }

    @Test
    @SuppressWarnings("Slf4jConstantLogMessage")
    public void upgradeFromOlderInternalSchemaDoesNotErrorOnTablesWithOldMetadata() {
        TableReference tableRef = TableReference.createFromFullyQualifiedName("test.oldTimeyTable");
        keyValueService.put(
                AtlasDbConstants.DEFAULT_METADATA_TABLE,
                ImmutableMap.of(CassandraKeyValueServices.getOldMetadataCell(tableRef), originalMetadata),
                System.currentTimeMillis());
        keyValueService.createTable(tableRef, originalMetadata);

        ((CassandraKeyValueServiceImpl) keyValueService).upgradeFromOlderInternalSchema();
        verify(logger, never()).error(anyString(), any(Object.class));
    }

    private void putDummyValueAtCellAndTimestamp(
            TableReference tableReference, Cell cell, long atlasTimestamp, long cassandraTimestamp)
            throws TException {
        CassandraKeyValueServiceImpl ckvs = (CassandraKeyValueServiceImpl) keyValueService;
        ckvs.getClientPool().runWithRetry(input -> {
            CqlQuery cqlQuery = CqlQuery.builder()
                    .safeQueryFormat("INSERT INTO \"%s\".\"%s\" (key, column1, column2, value)"
                            + " VALUES (%s, %s, %s, %s) USING TIMESTAMP %s;")
                    .addArgs(
                            SafeArg.of("keyspace", container.getConfig().getKeyspaceOrThrow()),
                            LoggingArgs.internalTableName(tableReference),
                            UnsafeArg.of("row", convertBytesToHexString(cell.getRowName())),
                            UnsafeArg.of("column", convertBytesToHexString(cell.getColumnName())),
                            SafeArg.of("atlasTimestamp", ~atlasTimestamp),
                            UnsafeArg.of("value", convertBytesToHexString(PtBytes.toBytes("testtesttest"))),
                            SafeArg.of("cassandraTimestamp", cassandraTimestamp))
                    .build();
            return input.execute_cql3_query(
                    cqlQuery,
                    Compression.NONE,
                    ConsistencyLevel.QUORUM);
        });
    }

    private String convertBytesToHexString(byte[] bytes) {
        return "0x" + BaseEncoding.base16().lowerCase().encode(bytes);
    }

    private void createExtraLocksTable(SchemaMutationLockTables lockTables,
            CassandraKeyValueServiceImpl kvs) throws TException {
        TableReference originalTable = Iterables.getOnlyElement(lockTables.getAllLockTables());

        try {
            kvs.createTable(TableReference.create(originalTable.getNamespace(),
                    "_locks_other"), AtlasDbConstants.EMPTY_TABLE_METADATA);
        } catch (UncheckedExecutionException ex) {
            // expected - we just created another locks table
        }

        assertThat(lockTables.getAllLockTables(), hasItem(not(originalTable)));
    }

    private static int getAmountOfGarbageInMetadataTable(KeyValueService keyValueService, TableReference tableRef) {
        return keyValueService.getAllTimestamps(
                AtlasDbConstants.DEFAULT_METADATA_TABLE,
                ImmutableSet.of(Cell.create(
                        tableRef.getQualifiedName().getBytes(StandardCharsets.UTF_8),
                        "m".getBytes(StandardCharsets.UTF_8))),
                AtlasDbConstants.MAX_TS).size();
    }
}
