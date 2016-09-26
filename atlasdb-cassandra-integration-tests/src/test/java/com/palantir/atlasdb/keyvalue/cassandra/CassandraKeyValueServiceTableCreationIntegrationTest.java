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
package com.palantir.atlasdb.keyvalue.cassandra;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;

public class CassandraKeyValueServiceTableCreationIntegrationTest {
    public static final TableReference GOOD_TABLE = TableReference.createFromFullyQualifiedName("foo.bar");
    public static final TableReference BAD_TABLE = TableReference.createFromFullyQualifiedName("foo.b@r");

    protected CassandraKeyValueService kvs;
    protected CassandraKeyValueService slowTimeoutKvs;

    @Before
    public void setUp() {
        ImmutableCassandraKeyValueServiceConfig quickTimeoutConfig = CassandraTestSuite.CASSANDRA_KVS_CONFIG
                .withSchemaMutationTimeoutMillis(500);
        kvs = CassandraKeyValueService.create(
                CassandraKeyValueServiceConfigManager.createSimpleManager(quickTimeoutConfig), CassandraTestSuite.LEADER_CONFIG);

        ImmutableCassandraKeyValueServiceConfig slowTimeoutConfig = CassandraTestSuite.CASSANDRA_KVS_CONFIG
                .withSchemaMutationTimeoutMillis(6 * 1000);
        slowTimeoutKvs = CassandraKeyValueService.create(
                CassandraKeyValueServiceConfigManager.createSimpleManager(slowTimeoutConfig), CassandraTestSuite.LEADER_CONFIG);

        kvs.dropTable(AtlasDbConstants.TIMESTAMP_TABLE);
    }

    @After
    public void tearDown() {
        kvs.teardown();
    }

    @Test (timeout = 10 * 1000)
    public void testTableCreationCanOccurAfterError() {
        try {
            kvs.createTable(BAD_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);
        } catch (Exception e) {
            // failure expected
        }
        kvs.createTable(GOOD_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);
        kvs.dropTable(GOOD_TABLE);
    }

    @Test
    public void testCreatingMultipleTablesAtOnce() throws InterruptedException {
        int threadCount =  16;
        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        ForkJoinPool threadPool = new ForkJoinPool(threadCount);

        threadPool.submit(() -> {
            IntStream.range(0, threadCount).parallel().forEach(i -> {
                try {
                    barrier.await();
                    slowTimeoutKvs.createTable(GOOD_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);
                } catch (BrokenBarrierException | InterruptedException e) {
                    // Do nothing
                }
            });
        });

        threadPool.shutdown();
        Preconditions.checkState(threadPool.awaitTermination(60, TimeUnit.SECONDS),
                "Not all table creation threads completed within the time limit");

        slowTimeoutKvs.dropTable(GOOD_TABLE);
    }

    @Test
    public void describeVersionBehavesCorrectly() throws Exception {
        kvs.clientPool.runWithRetry(CassandraVerifier.underlyingCassandraClusterSupportsCASOperations);
    }


    @Test
    public void testCreateTableCanRestoreLostMetadata() {
        // setup a basic table
        TableReference missingMetadataTable = TableReference.createFromFullyQualifiedName("test.metadata_missing");
        byte[] initialMetadata = new TableDefinition() {{
            rowName();
            rowComponent("blob", ValueType.BLOB);
            columns();
            column("bar", "b", ValueType.BLOB);
            conflictHandler(ConflictHandler.IGNORE_ALL);
            sweepStrategy(TableMetadataPersistence.SweepStrategy.NOTHING);
        }}.toTableMetadata().persistToBytes();

        kvs.createTable(missingMetadataTable, initialMetadata);


        // retrieve the metadata and see that it's the same as what we just put in
        byte[] existingMetadata = kvs.getMetadataForTable(missingMetadataTable);
        assertThat(initialMetadata, is(existingMetadata));

        // Directly get and delete the metadata (`get` necessary to get the fake timestamp putMetadataForTables used)
        Cell cell = Cell.create(
                missingMetadataTable.getQualifiedName().getBytes(Charset.defaultCharset()),
                "m".getBytes(StandardCharsets.UTF_8));
        Value persistedMetadata = Iterables.getLast(
                kvs.get(AtlasDbConstants.METADATA_TABLE, ImmutableMap.of(cell, Long.MAX_VALUE)).values());
        kvs.delete(AtlasDbConstants.METADATA_TABLE, ImmutableMultimap.of(cell, persistedMetadata.getTimestamp()));

        // pretend we started up again and did a createTable() for our existing table, that no longer has metadata
        kvs.createTable(missingMetadataTable, initialMetadata);

        // retrieve the metadata again and see that it's the same as what we just put in
        existingMetadata = kvs.getMetadataForTable(missingMetadataTable);
        assertThat(initialMetadata, is(existingMetadata));
    }

    @Test
    public void testGetMetadataCaseInsensitive() {
        // setup a basic table
        TableReference caseSensitiveTable = TableReference.createFromFullyQualifiedName("test.cased_table");
        TableReference wackyCasedTable = TableReference.createFromFullyQualifiedName("test.CaSeD_TaBlE");

        byte[] initialMetadata = new TableDefinition() {{
            rowName();
            rowComponent("blob", ValueType.BLOB);
            columns();
            column("bar", "b", ValueType.BLOB);
            conflictHandler(ConflictHandler.IGNORE_ALL);
            sweepStrategy(TableMetadataPersistence.SweepStrategy.NOTHING);
        }}.toTableMetadata().persistToBytes();

        kvs.createTable(caseSensitiveTable, initialMetadata);

        // retrieve the metadata and see that it's the same as what we just put in
        byte[] existingMetadata = kvs.getMetadataForTable(caseSensitiveTable);
        assertThat(initialMetadata, is(existingMetadata));

        // retrieve same metadata with a wacky cased version of the "same" name
        existingMetadata = kvs.getMetadataForTable(wackyCasedTable);
        assertThat(initialMetadata, is(existingMetadata));
    }
}
