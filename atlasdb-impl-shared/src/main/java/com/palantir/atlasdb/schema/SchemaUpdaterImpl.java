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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.ExpirationStrategy;
import com.palantir.atlasdb.schema.stream.StreamTables;
import com.palantir.atlasdb.table.description.IndexDefinition;
import com.palantir.atlasdb.table.description.RowNamePartitioner;
import com.palantir.atlasdb.table.description.Schemas;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionManager;
import com.palantir.common.collect.Maps2;

public class SchemaUpdaterImpl implements SchemaUpdater {
    private final SimpleSchemaUpdater delegate;
    private final Namespace namespace;
    private final KeyValueService kvs;
    private final LockAwareTransactionManager txManager;
    private final TaskProgress progress;
    private final SchemaVersion schemaVersion;
    private final ExecutorService executor;
    private final int readBatchSize;
    private final AtomicBoolean ranCopyTable = new AtomicBoolean();

    private SchemaUpdaterImpl(SimpleSchemaUpdater delegate,
                              Namespace namespace,
                              KeyValueService kvs,
                              LockAwareTransactionManager txManager,
                              TaskProgress progress,
                              SchemaVersion schemaVersion,
                              ExecutorService executor,
                              int readBatchSize) {
        this.delegate = delegate;
        this.namespace = namespace;
        this.kvs = kvs;
        this.txManager = txManager;
        this.progress = progress;
        this.schemaVersion = schemaVersion;
        this.executor = executor;
        this.readBatchSize = readBatchSize;
    }

    public static SchemaUpdater create(Namespace namespace,
                                       KeyValueService kvs,
                                       LockAwareTransactionManager txManager,
                                       TaskProgress progress,
                                       SchemaVersion schemaVersion,
                                       ExecutorService executor,
                                       int readBatchSize) {
        return new SchemaUpdaterImpl(SimpleSchemaUpdaterImpl.create(kvs, namespace), namespace,
                kvs, txManager, progress, schemaVersion, executor, readBatchSize);
    }

    @Override
    public void addTable(String tableName, TableDefinition definition) {
        delegate.addTable(tableName, definition);
    }

    @Override
    public void deleteTable(String tableName) {
        delegate.deleteTable(tableName);
    }

    @Override
    public void addIndex(String indexName, IndexDefinition definition) {
        delegate.addIndex(indexName, definition);
    }

    @Override
    public void deleteIndex(String indexName) {
        delegate.deleteIndex(indexName);
    }

    @Override
    public void updateTableMetadata(String tableName, TableDefinition definition) {
        delegate.updateTableMetadata(tableName, definition);
    }

    @Override
    public void updateIndexMetadata(String indexName, IndexDefinition definition) {
        delegate.updateIndexMetadata(indexName, definition);
    }

    @Override
    public boolean tableExists(String tableName) {
        return delegate.tableExists(tableName);
    }

    @Override
    public void changeRowName(String srcTable,
                              String destTable,
                              final Function<byte[], byte[]> rowNameTransform) {
        String fullSrcTable = getFullTableName(srcTable);
        String fullDestTable = getFullTableName(destTable);
        Preconditions.checkArgument(kvs.getAllTableNames().contains(fullSrcTable), "src table " + fullSrcTable + " does not exist");
        Preconditions.checkArgument(!fullSrcTable.equals(fullDestTable), "src and dest tables may not be equal");

        Function<RowResult<byte[]>, Map<Cell, byte[]>> rowTransformer = new Function<RowResult<byte[]>, Map<Cell,byte[]>>() {
            @Override
            public Map<Cell, byte[]> apply(RowResult<byte[]> input) {
                RowResult<byte[]> newRow = RowResult.create(rowNameTransform.apply(input.getRowName()), input.getColumns());
                return Maps2.fromEntries(newRow.getCells());
            }
        };
        copyTable(
                srcTable,
                destTable,
                rowTransformer,
                ColumnSelection.all());
    }

    @Override
    public void copyTable(String srcTable,
                          String destTable,
                          Function<RowResult<byte[]>, Map<Cell, byte[]>> rowTransformer,
                          ColumnSelection columnSelection) {
        String fullSrcTable = getFullTableName(srcTable);
        String fullDestTable = getFullTableName(destTable);
        Preconditions.checkState(!ranCopyTable.getAndSet(true), "Cannot copy two tables in the same upgrade task");

        byte[] metadata = kvs.getMetadataForTable(fullSrcTable);
        TableMetadata tableMeta = TableMetadata.BYTES_HYDRATOR.hydrateFromBytes(metadata);
        List<RowNamePartitioner> partitioners = tableMeta.getRowMetadata().getPartitionersForRow();

        UpgradeTaskCheckpointer checkpointer = new UpgradeTaskCheckpointer(
                namespace,
                schemaVersion,
                txManager);
        TransactionRangeMigrator rangeMigrator = new TransactionRangeMigratorBuilder().
                srcTable(fullSrcTable).
                destTable(fullDestTable).
                readBatchSize(readBatchSize).
                txManager(txManager).
                checkpointer(checkpointer).
                rowTransformer(rowTransformer).
                build();
        TableMigratorBuilder migratorBuilder = new TableMigratorBuilder().
                srcTable(fullSrcTable).
                partitions(256).
                partitioners(partitioners).
                readBatchSize(readBatchSize).
                executor(executor).
                checkpointer(checkpointer).
                columnSelection(columnSelection).
                rangeMigrator(rangeMigrator);
        if (progress != null) {
            migratorBuilder.progress(progress);
        }
        TableMigrator migrator = migratorBuilder.build();
        migrator.migrate();
    }

    @Override
    public LockAwareTransactionManager getTxManager() {
        return txManager;
    }

    @Override
    public void addStreamStore(String longName, String shortName, ValueType streamIdType) {
        delegate.addTable(shortName + StreamTables.METADATA_TABLE_SUFFIX, StreamTables.getStreamMetadataDefinition(longName, streamIdType, ExpirationStrategy.NEVER, false, false));
        delegate.addTable(shortName + StreamTables.VALUE_TABLE_SUFFIX, StreamTables.getStreamValueDefinition(longName, streamIdType, ExpirationStrategy.NEVER, false, false));
        delegate.addTable(shortName + StreamTables.HASH_TABLE_SUFFIX, StreamTables.getStreamHashIdxDefinition(longName, streamIdType, ExpirationStrategy.NEVER, false));
        delegate.addTable(shortName + StreamTables.INDEX_TABLE_SUFFIX, StreamTables.getStreamIdxDefinition(longName, streamIdType, ExpirationStrategy.NEVER, false, false));
    }

    @Override
    public void initializeSchema(final AtlasSchema schema) {
        delegate.initializeSchema(schema);
    }

    @Override
    public void resetTableMetadata(AtlasSchema schema, String tableName) {
        delegate.resetTableMetadata(schema, tableName);
    }

    @Override
    public void resetIndexMetadata(AtlasSchema schema, String indexName) {
        delegate.resetIndexMetadata(schema, indexName);
    }

    private String getFullTableName(String tableName) {
        if (tableName.startsWith(namespace.getName() + '.')) {
            return tableName;
        }
        return Schemas.getFullTableName(tableName, namespace);
    }
}
