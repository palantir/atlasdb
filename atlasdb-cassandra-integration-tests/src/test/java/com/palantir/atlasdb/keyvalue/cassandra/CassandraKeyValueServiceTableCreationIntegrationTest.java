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
package com.palantir.atlasdb.keyvalue.cassandra;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.MoreCollectors;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.containers.CassandraResource;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.AbstractKeyValueService;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.KsDef;
import org.apache.thrift.TException;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

public class CassandraKeyValueServiceTableCreationIntegrationTest {
    private static final TableReference GOOD_TABLE = TableReference.createFromFullyQualifiedName("foo.bar");
    private static final TableReference BAD_TABLE = TableReference.createFromFullyQualifiedName("foo.b@r");

    private static CassandraKeyValueService kvs;
    private static CassandraKeyValueService slowTimeoutKvs;

    @ClassRule
    public static final CassandraResource CASSANDRA = new CassandraResource();

    @BeforeClass
    public static void initializeKvs() {
        kvs = kvsWithSchemaMutationTimeout(500);
        CASSANDRA.registerKvs(kvs);
        slowTimeoutKvs = kvsWithSchemaMutationTimeout(6 * 1000);
        CASSANDRA.registerKvs(slowTimeoutKvs);
    }

    @Test(timeout = 10 * 1000)
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
        int threadCount = 16;
        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        ForkJoinPool threadPool = new ForkJoinPool(threadCount);

        threadPool.execute(() -> IntStream.range(0, threadCount).parallel().forEach(i -> {
            try {
                barrier.await();
                slowTimeoutKvs.createTable(GOOD_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);
            } catch (BrokenBarrierException | InterruptedException e) {
                // Do nothing
            }
        }));

        threadPool.shutdown();
        Preconditions.checkState(
                threadPool.awaitTermination(90, TimeUnit.SECONDS),
                "Not all table creation threads completed within the time limit");

        slowTimeoutKvs.dropTable(GOOD_TABLE);
    }

    @Test
    public void testCreateTableCanRestoreLostMetadata() {
        // setup a basic table
        TableReference missingMetadataTable = TableReference.createFromFullyQualifiedName("test.metadata_missing");
        byte[] initialMetadata = new TableDefinition() {
            {
                rowName();
                rowComponent("blob", ValueType.BLOB);
                columns();
                column("bar", "b", ValueType.BLOB);
                conflictHandler(ConflictHandler.IGNORE_ALL);
                sweepStrategy(TableMetadataPersistence.SweepStrategy.NOTHING);
            }
        }.toTableMetadata().persistToBytes();

        kvs.createTable(missingMetadataTable, initialMetadata);

        // retrieve the metadata and see that it's the same as what we just put in
        byte[] existingMetadata = kvs.getMetadataForTable(missingMetadataTable);
        assertThat(initialMetadata).isEqualTo(existingMetadata);

        // Directly get and delete the metadata (`get` necessary to get the fake timestamp putMetadataForTables used)
        Cell cell = Cell.create(
                missingMetadataTable.getQualifiedName().getBytes(StandardCharsets.UTF_8),
                "m".getBytes(StandardCharsets.UTF_8));
        Value persistedMetadata = Iterables.getLast(
                kvs.get(AtlasDbConstants.DEFAULT_METADATA_TABLE, ImmutableMap.of(cell, Long.MAX_VALUE))
                        .values());
        kvs.delete(
                AtlasDbConstants.DEFAULT_METADATA_TABLE, ImmutableMultimap.of(cell, persistedMetadata.getTimestamp()));

        // pretend we started up again and did a createTable() for our existing table, that no longer has metadata
        kvs.createTable(missingMetadataTable, initialMetadata);

        // retrieve the metadata again and see that it's the same as what we just put in
        existingMetadata = kvs.getMetadataForTable(missingMetadataTable);
        assertThat(initialMetadata).isEqualTo(existingMetadata);
    }

    @Test
    public void testGetMetadataCaseInsensitive() {
        // Make two casewise-different references to the "same" table
        TableReference caseSensitiveTable = TableReference.createFromFullyQualifiedName("test.cased_table");
        TableReference wackyCasedTable = TableReference.createFromFullyQualifiedName("test.CaSeD_TaBlE");

        byte[] initialMetadata = new TableDefinition() {
            {
                rowName();
                rowComponent("blob", ValueType.BLOB);
                columns();
                column("bar", "b", ValueType.BLOB);
                conflictHandler(ConflictHandler.IGNORE_ALL);
                sweepStrategy(TableMetadataPersistence.SweepStrategy.NOTHING);
            }
        }.toTableMetadata().persistToBytes();

        kvs.createTable(caseSensitiveTable, initialMetadata);

        // retrieve the metadata and see that it's the same as what we just put in
        byte[] existingMetadata = kvs.getMetadataForTable(caseSensitiveTable);
        assertThat(initialMetadata).isEqualTo(existingMetadata);

        // retrieve same metadata with a wacky cased version of the "same" name
        existingMetadata = kvs.getMetadataForTable(wackyCasedTable);
        assertThat(initialMetadata).isEqualTo(existingMetadata);

        kvs.dropTable(caseSensitiveTable);
    }

    @Test
    public void testCreateTransactions2TableProducesCorrectMetadata() throws TException {
        kvs.createTable(
                TransactionConstants.TRANSACTIONS2_TABLE,
                TransactionConstants.TRANSACTIONS2_TABLE_METADATA.persistToBytes());

        KsDef ksDef = kvs.getClientPool()
                .run(client -> client.describe_keyspace(CASSANDRA.getConfig().getKeyspaceOrThrow()));
        CfDef transactions2CfDef = ksDef.cf_defs.stream()
                .filter(cfDef -> cfDef.name.equals(
                        AbstractKeyValueService.internalTableName(TransactionConstants.TRANSACTIONS2_TABLE)))
                .collect(MoreCollectors.onlyElement());
        assertThat(transactions2CfDef.bloom_filter_fp_chance)
                .isEqualTo(CassandraConstants.DENSELY_ACCESSED_WIDE_ROWS_BLOOM_FILTER_FP_CHANCE);
        assertThat(transactions2CfDef.min_index_interval)
                .isEqualTo(CassandraConstants.DENSELY_ACCESSED_WIDE_ROWS_INDEX_INTERVAL);
        assertThat(transactions2CfDef.max_index_interval)
                .isEqualTo(CassandraConstants.DENSELY_ACCESSED_WIDE_ROWS_INDEX_INTERVAL);
        assertThat(transactions2CfDef.compression_options.get(CassandraConstants.CFDEF_COMPRESSION_CHUNK_LENGTH_KEY))
                .isEqualTo(String.valueOf(
                        TransactionConstants.TRANSACTIONS2_TABLE_METADATA.getExplicitCompressionBlockSizeKB()));
    }

    private static CassandraKeyValueService kvsWithSchemaMutationTimeout(int millis) {
        ImmutableCassandraKeyValueServiceConfig config = ImmutableCassandraKeyValueServiceConfig.copyOf(
                        CASSANDRA.getConfig())
                .withSchemaMutationTimeoutMillis(millis);
        return CassandraKeyValueServiceImpl.createForTesting(config);
    }
}
