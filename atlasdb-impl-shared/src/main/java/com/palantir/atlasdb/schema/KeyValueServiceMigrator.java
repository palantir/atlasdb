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


import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.table.description.RowNamePartitioner;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.common.base.Throwables;
import com.palantir.common.concurrent.PTExecutors;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class KeyValueServiceMigrator {
    private final TableReference checkpointTable;
    private static final int PARTITIONS = 256;

    private final TransactionManager fromTransactionManager;
    private final TransactionManager toTransactionManager;
    private final KeyValueService fromKvs;
    private final KeyValueService toKvs;
    private final Supplier<Long> migrationTimestampSupplier;

    private final int threads;
    private final int defaultBatchSize;

    // Tables that exist on the legacy KVS and should not be migrated.
    // TODO(tgordeeva): hacky, clean this up when we have table specific migration
    private final Set<TableReference> unmigratableTables;

    private final Map<TableReference, Integer> readBatchSizeOverrides;

    public enum KvsMigrationMessageLevel {
        INFO,
        WARN,
        ERROR
    }

    public interface KvsMigrationMessageProcessor {
        void processMessage(String message, KvsMigrationMessageLevel level);
    }

    private final KvsMigrationMessageProcessor messageProcessor;
    private final TaskProgress taskProgress;

    public KeyValueServiceMigrator(Namespace checkpointNamespace,
            TransactionManager fromTransactionManager,
            TransactionManager toTransactionManager,
            KeyValueService fromKvs,
            KeyValueService toKvs,
            Supplier<Long> migrationTimestampSupplier,
            int threads,
            int defaultBatchSize,
            Map<TableReference, Integer> readBatchSizeOverrides,
            KvsMigrationMessageProcessor messageProcessor,
            TaskProgress taskProgress,
            Set<TableReference> unmigratableTables) {
        this.checkpointTable =
                TableReference.create(checkpointNamespace, KeyValueServiceMigratorUtils.CHECKPOINT_TABLE_NAME);
        this.fromTransactionManager = fromTransactionManager;
        this.toTransactionManager = toTransactionManager;
        this.fromKvs = fromKvs;
        this.toKvs = toKvs;
        this.migrationTimestampSupplier = migrationTimestampSupplier;
        this.threads = threads;
        this.defaultBatchSize = defaultBatchSize;
        this.readBatchSizeOverrides = readBatchSizeOverrides;
        this.messageProcessor = messageProcessor;
        this.taskProgress = taskProgress;
        this.unmigratableTables = unmigratableTables;
    }

    private void processMessage(String string, KvsMigrationMessageLevel level) {
        KeyValueServiceMigratorUtils.processMessage(messageProcessor, string, level);
    }

    private void processMessage(String string, Throwable ex, KvsMigrationMessageLevel level) {
        KeyValueServiceMigratorUtils.processMessage(messageProcessor, string, ex, level);
    }

    /**
     * Drop and create tables before starting migration. Note that we do not want to drop atomic tables since there
     * are legacy use cases where a migration is done using a
     * {@link com.palantir.atlasdb.keyvalue.impl.TableSplittingKeyValueService} that delegates all atomic tables access
     * (including dropTable) back to the source KVS.
     */
    public void setup() {
        Set<TableReference> tablesToDrop = KeyValueServiceMigratorUtils.getCreatableTables(toKvs, unmigratableTables);
        processMessage("dropping tables: " + tablesToDrop, KvsMigrationMessageLevel.INFO);
        toKvs.dropTables(tablesToDrop);

        Map<TableReference, byte[]> metadataByTableName = Maps.newHashMap();
        for (TableReference tableRef : KeyValueServiceMigratorUtils.getCreatableTables(fromKvs, unmigratableTables)) {
            processMessage("retrieving metadata for table " + tableRef, KvsMigrationMessageLevel.INFO);
            byte[] metadata = fromKvs.getMetadataForTable(tableRef);
            Preconditions.checkArgument(
                    metadata != null && metadata.length != 0,
                    "no metadata found for table %s", tableRef);
            metadataByTableName.put(tableRef, metadata);
        }
        processMessage("creating tables", KvsMigrationMessageLevel.INFO);
        toKvs.createTables(metadataByTableName);
        processMessage("setup complete", KvsMigrationMessageLevel.INFO);



    }

    public void migrate() {
        try {
            internalMigrate();
        } catch (InterruptedException e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    /**
     * On success, delete the checkpoint table.
     */
    public void cleanup() {
        GeneralTaskCheckpointer checkpointer =
                new GeneralTaskCheckpointer(checkpointTable, toKvs, toTransactionManager);

        processMessage("Deleting checkpoint table...", KvsMigrationMessageLevel.INFO);
        checkpointer.deleteCheckpoints();

        processMessage("Migration complete.", KvsMigrationMessageLevel.INFO);
    }

    private void internalMigrate() throws InterruptedException {
        Set<TableReference> tables = KeyValueServiceMigratorUtils.getMigratableTableNames(fromKvs, unmigratableTables,
                checkpointTable);

        GeneralTaskCheckpointer checkpointer =
                new GeneralTaskCheckpointer(checkpointTable, toKvs, toTransactionManager);

        ExecutorService executor = PTExecutors.newFixedThreadPool(threads);
        try {
            migrateTables(
                    tables,
                    fromTransactionManager,
                    toTransactionManager,
                    toKvs,
                    migrationTimestampSupplier.get(),
                    executor,
                    checkpointer);
            processMessage("Data migration complete.", KvsMigrationMessageLevel.INFO);
        } catch (Throwable t) {
            processMessage("Migration failed.", t, KvsMigrationMessageLevel.ERROR);
            throw Throwables.throwUncheckedException(t);
        } finally {
            executor.shutdown();
            executor.awaitTermination(10000L, TimeUnit.MILLISECONDS);
        }
    }

    private void migrateTables(Set<TableReference> tables,
                               TransactionManager readTxManager,
                               TransactionManager txManager,
                               KeyValueService writeKvs,
                               long migrationTimestamp,
                               ExecutorService executor,
                               GeneralTaskCheckpointer checkpointer) {
        processMessage("Migrating tables at migrationTimestamp " + migrationTimestamp,
                KvsMigrationMessageLevel.INFO);
        for (TableReference table : tables) {
            KvsRangeMigrator rangeMigrator =
                    new KvsRangeMigratorBuilder().srcTable(table).readBatchSize(getBatchSize(table)).readTxManager(
                            readTxManager).txManager(txManager).writeKvs(writeKvs).migrationTimestamp(
                            migrationTimestamp).checkpointer(checkpointer).build();
            TableMigratorBuilder builder =
                    new TableMigratorBuilder().srcTable(table).partitions(PARTITIONS).partitioners(
                            getPartitioners(fromKvs, table)).readBatchSize(
                            getBatchSize(table)).executor(executor).checkpointer(checkpointer).progress(
                            taskProgress).rangeMigrator(rangeMigrator);
            TableMigrator migrator = builder.build();
            migrator.migrate();
        }
    }

    private List<RowNamePartitioner> getPartitioners(KeyValueService kvs, TableReference table) {
        try {
            byte[] metadata = kvs.getMetadataForTable(table);
            TableMetadata tableMeta = TableMetadata.BYTES_HYDRATOR.hydrateFromBytes(metadata);
            return tableMeta.getRowMetadata().getPartitionersForRow();
        } catch (RuntimeException e) {
            processMessage(
                    "Could not resolve partitioners from table metadata for "
                            + table
                            + " this may result in a small decrease in performance migrating this table.",
                    e,
                    KvsMigrationMessageLevel.WARN);
            return ImmutableList.of();
        }
    }

    private int getBatchSize(TableReference table) {
        Integer batchSize = readBatchSizeOverrides.get(table);
        return batchSize != null ? batchSize : defaultBatchSize;
    }
}
