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
package com.palantir.atlasdb.table.description;

import static java.util.stream.Collectors.toSet;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cleaner.api.OnCleanupTask;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.AbstractKeyValueService;
import com.palantir.atlasdb.schema.stream.StreamStoreDefinition;
import com.palantir.atlasdb.table.description.IndexDefinition.IndexType;
import com.palantir.atlasdb.table.description.render.StreamStoreRenderer;
import com.palantir.atlasdb.table.description.render.TableFactoryRenderer;
import com.palantir.atlasdb.table.description.render.TableRenderer;
import com.palantir.atlasdb.table.description.render.TableRendererV2;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.lock.watch.LockWatchReferences;
import com.palantir.lock.watch.LockWatchReferences.LockWatchReference;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * Defines a schema.
 *
 * A schema consists of table definitions and indexes.
 *
 * Schema objects can be used for creating/dropping tables within key values
 * stores, as well as compiling automatically generated code for accessing
 * tables in a type-safe fashion.
 */
@SuppressWarnings("checkstyle:Indentation")
public class Schema {
    private static final SafeLogger log = SafeLoggerFactory.get(Schema.class);

    private final String name;
    private final String packageName;
    private final Namespace namespace;
    private final OptionalType optionalType;
    private boolean ignoreTableNameLengthChecks = false;

    private final Multimap<String, Supplier<OnCleanupTask>> cleanupTasks = ArrayListMultimap.create();
    private final Map<String, TableDefinition> tableDefinitions = new HashMap<>();
    private final Map<String, IndexDefinition> indexDefinitions = new HashMap<>();
    private final List<StreamStoreRenderer> streamStoreRenderers = new ArrayList<>();
    private final Set<LockWatchReference> lockWatches = new HashSet<>();

    // N.B., the following is a list multimap because we want to preserve order
    // for code generation purposes.
    private final ListMultimap<String, String> indexesByTable = ArrayListMultimap.create();

    /** Creates a new schema, using Guava Optionals. */
    public Schema(Namespace namespace) {
        this(null, null, namespace);
    }

    /** Creates a new schema, using Guava Optionals. */
    public Schema() {
        this(null, null, Namespace.DEFAULT_NAMESPACE);
    }

    /** Creates a new schema, using Guava Optionals. */
    public Schema(String name, String packageName, Namespace namespace) {
        this(name, packageName, namespace, OptionalType.GUAVA);
    }

    public Schema(String name, String packageName, Namespace namespace, OptionalType optionalType) {
        this.name = name;
        this.packageName = packageName;
        this.namespace = namespace;
        this.optionalType = optionalType;
    }

    public String getName() {
        return name;
    }

    public void addTableDefinition(String tableName, TableDefinition definition) {
        Preconditions.checkArgument(
                !tableDefinitions.containsKey(tableName) && !indexDefinitions.containsKey(tableName),
                "Table already defined: %s",
                tableName);
        Preconditions.checkArgument(Schemas.isTableNameValid(tableName), "Invalid table name %s", tableName);
        validateTableNameLength(tableName);
        if (definition.enableCaching) {
            com.palantir.logsafe.Preconditions.checkArgument(
                    definition.conflictHandler == ConflictHandler.SERIALIZABLE_CELL,
                    "Caching can only be enabled with the SERIALIZABLE_CELL conflict handler.");
            lockWatches.add(LockWatchReferences.entireTable(
                    TableReference.create(namespace, tableName).getQualifiedName()));
        }
        tableDefinitions.put(tableName, definition);
    }

    public void addDefinitionsForTables(Iterable<String> tableNames, TableDefinition definition) {
        for (String t : tableNames) {
            addTableDefinition(t, definition);
        }
    }

    public TableDefinition getTableDefinition(TableReference tableRef) {
        return tableDefinitions.get(tableRef.getTablename());
    }

    public Map<TableReference, TableMetadata> getAllTablesAndIndexMetadata() {
        Map<TableReference, TableMetadata> ret = new HashMap<>();
        for (Map.Entry<String, TableDefinition> e : tableDefinitions.entrySet()) {
            ret.put(TableReference.create(namespace, e.getKey()), e.getValue().toTableMetadata());
        }
        for (Map.Entry<String, IndexDefinition> e : indexDefinitions.entrySet()) {
            ret.put(
                    TableReference.create(namespace, e.getKey()),
                    e.getValue().toIndexMetadata(e.getKey()).getTableMetadata());
        }
        return ret;
    }

    public Set<TableReference> getAllIndexes() {
        return indexDefinitions.keySet().stream()
                .map(table -> TableReference.create(namespace, table))
                .collect(toSet());
    }

    public Set<TableReference> getAllTables() {
        return tableDefinitions.keySet().stream()
                .map(table -> TableReference.create(namespace, table))
                .collect(toSet());
    }

    public void addIndexDefinition(String idxName, IndexDefinition definition) {
        validateIndex(idxName, definition);
        String indexName = Schemas.appendIndexSuffix(idxName, definition).getQualifiedName();
        validateTableNameLength(indexName);
        indexesByTable.put(definition.getSourceTable(), indexName);
        indexDefinitions.put(indexName, definition);
    }

    private void validateTableNameLength(String idxName) {
        if (!ignoreTableNameLengthChecks) {
            String internalTableName =
                    AbstractKeyValueService.internalTableName(TableReference.create(namespace, idxName));
            List<CharacterLimitType> kvsExceeded = new ArrayList<>();

            if (internalTableName.length() > AtlasDbConstants.CASSANDRA_TABLE_NAME_CHAR_LIMIT) {
                kvsExceeded.add(CharacterLimitType.CASSANDRA);
            }
            if (internalTableName.length() > AtlasDbConstants.POSTGRES_TABLE_NAME_CHAR_LIMIT) {
                kvsExceeded.add(CharacterLimitType.POSTGRES);
            }
            Preconditions.checkArgument(
                    kvsExceeded.isEmpty(),
                    "Internal table name %s is too long, known to exceed character limits for "
                            + "the following KVS: %s. If using a table prefix, please ensure that the concatenation "
                            + "of the prefix with the internal table name is below the KVS limit. "
                            + "If running only against a different KVS, set the ignoreTableNameLength flag.",
                    idxName,
                    StringUtils.join(kvsExceeded, ", "));
        }
    }

    /**
     * Adds the given stream store to your schema.
     *
     * @param streamStoreDefinition You probably want to use a @{StreamStoreDefinitionBuilder} for convenience.
     */
    public void addStreamStoreDefinition(StreamStoreDefinition streamStoreDefinition) {
        streamStoreDefinition.getTables().forEach(this::addTableDefinition);
        StreamStoreRenderer renderer = streamStoreDefinition.getRenderer(packageName, name);
        Multimap<String, Supplier<OnCleanupTask>> streamStoreCleanupTasks =
                streamStoreDefinition.getCleanupTasks(packageName, name, renderer, namespace);

        cleanupTasks.putAll(streamStoreCleanupTasks);
        streamStoreRenderers.add(renderer);
    }

    private void validateIndex(String idxName, IndexDefinition definition) {
        for (IndexType type : IndexType.values()) {
            Preconditions.checkArgument(
                    !idxName.endsWith(type.getIndexSuffix()),
                    "Index name cannot end with '%s'.",
                    type.getIndexSuffix());
            String indexName = idxName + type.getIndexSuffix();
            com.palantir.logsafe.Preconditions.checkArgument(
                    !tableDefinitions.containsKey(indexName) && !indexDefinitions.containsKey(indexName),
                    "Table already defined.");
        }
        com.palantir.logsafe.Preconditions.checkArgument(
                tableDefinitions.containsKey(definition.getSourceTable()), "Index source table undefined.");
        Preconditions.checkArgument(Schemas.isTableNameValid(idxName), "Invalid table name %s", idxName);
        com.palantir.logsafe.Preconditions.checkArgument(
                !tableDefinitions
                                .get(definition.getSourceTable())
                                .toTableMetadata()
                                .getColumns()
                                .hasDynamicColumns()
                        || !definition.getIndexType().equals(IndexType.CELL_REFERENCING),
                "Cell referencing indexes not implemented for tables with dynamic columns.");
    }

    public IndexDefinition getIndex(TableReference indexRef) {
        return indexDefinitions.get(indexRef.getTablename());
    }

    /**
     * Performs some basic checks on this schema to check its validity.
     */
    public void validate() {
        // Try converting to metadata to see if any validation logic throws.
        for (Map.Entry<String, TableDefinition> entry : tableDefinitions.entrySet()) {
            try {
                entry.getValue().validate();
            } catch (Exception e) {
                log.error("Failed to validate table {}.", UnsafeArg.of("tableName", entry.getKey()), e);
                throw e;
            }
        }

        for (Map.Entry<String, IndexDefinition> indexEntry : indexDefinitions.entrySet()) {
            IndexDefinition def = indexEntry.getValue();
            try {
                def.toIndexMetadata(indexEntry.getKey()).getTableMetadata();
                def.validate();
            } catch (Exception e) {
                log.error("Failed to validate index {}.", UnsafeArg.of("indexName", indexEntry.getKey()), e);
                throw e;
            }
        }

        for (Map.Entry<String, String> e : indexesByTable.entries()) {
            TableMetadata tableMetadata = tableDefinitions.get(e.getKey()).toTableMetadata();

            Collection<String> rowNames = Collections2.transform(
                    tableMetadata.getRowMetadata().getRowParts(), NameComponentDescription::getComponentName);

            IndexMetadata indexMetadata = indexDefinitions.get(e.getValue()).toIndexMetadata(e.getValue());
            for (IndexComponent c :
                    Iterables.concat(indexMetadata.getRowComponents(), indexMetadata.getColumnComponents())) {
                if (c.rowComponentName != null) {
                    com.palantir.logsafe.Preconditions.checkArgument(
                            rowNames.contains(c.rowComponentName),
                            "In index, a componentFromRow must reference an existing row component");
                }
            }

            if (indexMetadata.getColumnNameToAccessData() != null) {
                com.palantir.logsafe.Preconditions.checkArgument(
                        tableMetadata.getColumns().getDynamicColumn() == null,
                        "Indexes accessing columns not supported for tables with dynamic columns.");
                Collection<String> columnNames = Collections2.transform(
                        tableMetadata.getColumns().getNamedColumns(), NamedColumnDescription::getLongName);
                com.palantir.logsafe.Preconditions.checkArgument(
                        columnNames.contains(indexMetadata.getColumnNameToAccessData()),
                        "In index, a component derived from column must reference an existing column");
            }

            if (indexMetadata.getIndexType().equals(IndexType.CELL_REFERENCING)) {
                com.palantir.logsafe.Preconditions.checkArgument(
                        ConflictHandler.RETRY_ON_WRITE_WRITE.equals(tableMetadata.getConflictHandler()),
                        "Nonadditive indexes require write-write conflicts on their tables");
            }
        }
    }

    public Map<TableReference, TableDefinition> getTableDefinitions() {
        return tableDefinitions.entrySet().stream()
                .collect(Collectors.toMap(e -> TableReference.create(namespace, e.getKey()), Map.Entry::getValue));
    }

    public Set<LockWatchReference> getLockWatches() {
        return lockWatches;
    }

    public Map<TableReference, IndexDefinition> getIndexDefinitions() {
        return indexDefinitions.entrySet().stream()
                .collect(Collectors.toMap(e -> TableReference.create(namespace, e.getKey()), Map.Entry::getValue));
    }

    public Namespace getNamespace() {
        return namespace;
    }

    /**
     * Performs code generation.
     *
     * @param srcDir root source directory where code generation is performed.
     */
    public void renderTables(File srcDir) throws IOException {
        com.palantir.logsafe.Preconditions.checkNotNull(name, "schema name not set");
        com.palantir.logsafe.Preconditions.checkNotNull(packageName, "package name not set");

        FileUtils.deleteDirectory(srcDir);

        TableRenderer tableRenderer = new TableRenderer(packageName, namespace, optionalType);
        TableRendererV2 tableRendererV2 = new TableRendererV2(packageName, namespace);
        for (Map.Entry<String, TableDefinition> entry : tableDefinitions.entrySet()) {
            String rawTableName = entry.getKey();
            TableDefinition table = entry.getValue();
            ImmutableSortedSet.Builder<IndexMetadata> indices = ImmutableSortedSet.orderedBy(
                    Ordering.natural().onResultOf((Function<IndexMetadata, String>) IndexMetadata::getIndexName));
            if (table.getGenericTableName() != null) {
                com.palantir.logsafe.Preconditions.checkState(
                        !indexesByTable.containsKey(rawTableName), "Generic tables cannot have indices");
            } else {
                for (String indexName : indexesByTable.get(rawTableName)) {
                    indices.add(indexDefinitions.get(indexName).toIndexMetadata(indexName));
                }
            }
            emit(
                    srcDir,
                    tableRenderer.render(rawTableName, table, indices.build()),
                    packageName,
                    tableRenderer.getClassName(rawTableName, table));
            if (table.hasV2TableEnabled()) {
                emit(
                        srcDir,
                        tableRendererV2.render(rawTableName, table),
                        packageName,
                        tableRendererV2.getClassName(rawTableName, table));
            }
        }
        for (StreamStoreRenderer renderer : streamStoreRenderers) {
            emit(srcDir, renderer.renderStreamStore(), renderer.getPackageName(), renderer.getStreamStoreClassName());
            emit(
                    srcDir,
                    renderer.renderIndexCleanupTask(),
                    renderer.getPackageName(),
                    renderer.getIndexCleanupTaskClassName());
            emit(
                    srcDir,
                    renderer.renderMetadataCleanupTask(),
                    renderer.getPackageName(),
                    renderer.getMetadataCleanupTaskClassName());
        }
        TableFactoryRenderer tableFactoryRenderer =
                TableFactoryRenderer.of(name, packageName, namespace, tableDefinitions);
        emit(
                srcDir,
                tableFactoryRenderer.render(),
                tableFactoryRenderer.getPackageName(),
                tableFactoryRenderer.getClassName());
    }

    private void emit(File srcDir, String code, String packName, String className) throws IOException {
        File outputDir = new File(srcDir, packName.replace(".", "/"));
        File outputFile = new File(outputDir, className + ".java");

        // create paths if they don't exist
        outputDir.mkdirs();
        outputFile = outputFile.getAbsoluteFile();
        outputFile.createNewFile();
        FileWriter os = null;
        try {
            os = new FileWriter(outputFile, StandardCharsets.UTF_8);
            os.write(code);
        } finally {
            if (os != null) {
                os.close();
            }
        }
    }

    // Cannot be removed, as it is used by the large internal product
    public void addCleanupTask(String rawTableName, OnCleanupTask task) {
        addCleanupTask(rawTableName, Suppliers.ofInstance(task));
    }

    public void addCleanupTask(String rawTableName, Supplier<OnCleanupTask> task) {
        cleanupTasks.put(rawTableName, task);
    }

    public Multimap<TableReference, OnCleanupTask> getCleanupTasksByTable() {
        Multimap<TableReference, OnCleanupTask> ret = ArrayListMultimap.create();
        for (Map.Entry<String, Supplier<OnCleanupTask>> e : cleanupTasks.entries()) {
            ret.put(TableReference.create(namespace, e.getKey()), e.getValue().get());
        }
        return ret;
    }

    public void ignoreTableNameLengthChecks() {
        ignoreTableNameLengthChecks = true;
    }
}
