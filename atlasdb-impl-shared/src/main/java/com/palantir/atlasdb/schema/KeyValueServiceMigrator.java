/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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


import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.table.description.RowNamePartitioner;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.common.base.Throwables;
import com.palantir.common.concurrent.PTExecutors;

public class KeyValueServiceMigrator {
    private final TableReference checkpointTable;
    private static final String CHECKPOINT_TABLE_NAME = "tmp_migrate_progress";
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
        this.checkpointTable = TableReference.create(checkpointNamespace, CHECKPOINT_TABLE_NAME);
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
        KeyValueServiceMigrators.processMessage(messageProcessor, string, level);
    }

    private void processMessage(String string, Throwable ex, KvsMigrationMessageLevel level) {
        KeyValueServiceMigrators.processMessage(messageProcessor, string, ex, level);
    }

    /**
     * Drop and create tables before starting migration.
     */
    public void setup() {
        /*
         * We do *not* want to drop tables that aren't getting migrated (since that would drop them
         * ON THE SOURCE KVS), but we *do* actually want to create metadata for those tables,
         * because they might be migrated later by dbcopy.
         */
        processMessage("dropping tables: " + getCreatableTableNames(toKvs), KvsMigrationMessageLevel.INFO);
        toKvs.dropTables(getCreatableTableNames(toKvs));

        Map<TableReference, byte[]> metadataByTableName = Maps.newHashMap();
        for (TableReference tableRef : getCreatableTableNames(fromKvs)) {
            processMessage("retrieving metadata for table " + tableRef, KvsMigrationMessageLevel.INFO);
            byte[] metadata = fromKvs.getMetadataForTable(tableRef);
            Preconditions.checkArgument(
                    metadata != null && metadata.length != 0,
                    "no metadata found for table " + tableRef);
            metadataByTableName.put(tableRef, metadata);
        }
        processMessage("creating tables", KvsMigrationMessageLevel.INFO);
        toKvs.createTables(metadataByTableName);
        processMessage("setup complete", KvsMigrationMessageLevel.INFO);
    }

    /**
     * Tables that are eligible for dropping and creating.
     */
    private Set<TableReference> getCreatableTableNames(KeyValueService kvs) {
        /*
         * Tables that cannot be migrated because they are not controlled by the transaction table,
         * but that don't necessarily live on the legacy DB KVS, should still be created on the new
         * KVS, even if they don't get populated. That's why this method is subtly different from
         * getMigratableTableNames().
         */
        Set<TableReference> tableNames = Sets.newHashSet(kvs.getAllTableNames());
        tableNames.removeAll(AtlasDbConstants.ATOMIC_TABLES);
        tableNames.removeAll(unmigratableTables);
        return tableNames;
    }

    public void migrate() {
        try {
            internalMigrate();
        } catch (InterruptedException e) {
            Throwables.throwUncheckedException(e);
        }
    }

    /**
     * On success, delete the checkpoint table.
     */
    public void cleanup() {
        TransactionManager txManager = toTransactionManager;

        GeneralTaskCheckpointer checkpointer =
                new GeneralTaskCheckpointer(checkpointTable, toKvs, txManager);

        processMessage("Deleting checkpoint table...", KvsMigrationMessageLevel.INFO);
        checkpointer.deleteCheckpoints();

        processMessage("Migration complete.", KvsMigrationMessageLevel.INFO);
    }

    private void internalMigrate() throws InterruptedException {
        Set<TableReference> tables = KeyValueServiceMigrators.getMigratableTableNames(fromKvs, unmigratableTables);
        TransactionManager txManager = toTransactionManager;

        TransactionManager readTxManager = fromTransactionManager;

        GeneralTaskCheckpointer checkpointer =
                new GeneralTaskCheckpointer(checkpointTable, toKvs, txManager);

        ExecutorService executor = PTExecutors.newFixedThreadPool(threads);
        try {
            migrateTables(
                    tables,
                    readTxManager,
                    txManager,
                    toKvs,
                    migrationTimestampSupplier.get(),
                    executor,
                    checkpointer);
            processMessage("Data migration complete.", KvsMigrationMessageLevel.INFO);
        } catch (Throwable t) {
            processMessage("Migration failed.", t, KvsMigrationMessageLevel.ERROR);
            Throwables.throwUncheckedException(t);
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
