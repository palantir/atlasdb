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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public final class Schemas {
    private static final String INDEX_SUFFIX = "idx";

    private Schemas() {
        // utility
    }

    public static TableReference appendIndexSuffix(String indexName, IndexDefinition definition) {
        Preconditions.checkArgument(
                !indexName.endsWith(INDEX_SUFFIX), "Index name cannot end with '%s': %s", INDEX_SUFFIX, indexName);
        return TableReference.createUnsafe(indexName + definition.getIndexType().getIndexSuffix());
    }

    public static void createIndices(
            KeyValueService kvs, Map<TableReference, IndexDefinition> fullIndexNameToDefinition) {
        Map<TableReference, byte[]> fullIndexNameToMetadata =
                Maps.newHashMapWithExpectedSize(fullIndexNameToDefinition.size());
        for (Map.Entry<TableReference, IndexDefinition> indexEntry : fullIndexNameToDefinition.entrySet()) {
            fullIndexNameToMetadata.put(
                    indexEntry.getKey(),
                    indexEntry
                            .getValue()
                            .toIndexMetadata(indexEntry.getKey().getQualifiedName())
                            .getTableMetadata()
                            .persistToBytes());
        }
        kvs.createTables(fullIndexNameToMetadata);
    }

    public static void createTable(KeyValueService kvs, TableReference tableRef, TableDefinition definition) {
        createTables(kvs, ImmutableMap.of(tableRef, definition));
    }

    public static void createTable(Schema schema, KeyValueService kvs, TableReference tableRef) {
        TableDefinition definition = schema.getTableDefinition(tableRef);
        createTable(kvs, tableRef, definition);
    }

    public static void createTables(KeyValueService kvs, Map<TableReference, TableDefinition> tableRefToDefinition) {
        Map<TableReference, byte[]> tableRefToMetadata = Maps.newHashMapWithExpectedSize(tableRefToDefinition.size());
        for (Map.Entry<TableReference, TableDefinition> tableEntry : tableRefToDefinition.entrySet()) {
            tableRefToMetadata.put(
                    tableEntry.getKey(), tableEntry.getValue().toTableMetadata().persistToBytes());
        }
        kvs.createTables(tableRefToMetadata);
    }

    public static String getTableReferenceString(String tableName, Namespace namespace) {
        Preconditions.checkArgument(isTableNameValid(tableName), "%s is not a valid table name", tableName);
        String namespaceName = namespace.getName();
        // Hacks for schemas that were created before namespaces were created.
        if (namespace.isEmptyNamespace() || namespaceName.equals("met")) {
            return "TableReference.createWithEmptyNamespace(\"" + tableName + "\")";
        } else {
            return "TableReference.createFromFullyQualifiedName(\"" + namespace.getName() + "." + tableName + "\")";
        }
    }

    public static boolean isTableNameValid(String tableName) {
        for (int i = 0; i < tableName.length(); i++) {
            char ch = tableName.charAt(i);
            if (!Character.isLetterOrDigit(ch) && ch != '_') {
                return false;
            }
        }
        return true;
    }

    /**
     * Creates tables/indexes for this schema.
     *
     * This operation is idempotent, so it can be called multiple times without
     * effect. Behavior is undefined if the schema has changed between calls
     * (e.g., it is not the responsibility of this method to perform schema
     * upgrades).
     */
    public static void createTablesAndIndexes(Schema schema, KeyValueService kvs) {
        schema.validate();
        createTables(kvs, schema.getTableDefinitions());
        createIndices(kvs, schema.getIndexDefinitions());
    }

    public static void deleteTablesAndIndexes(Schema schema, KeyValueService kvs) {
        schema.validate();
        kvs.dropTables(getExistingTablesAlsoPresentInSchema(schema, kvs));
    }

    /** intended for use by tests. **/
    public static void truncateTablesAndIndexes(Schema schema, KeyValueService kvs) {
        schema.validate();
        kvs.truncateTables(getExistingTablesAlsoPresentInSchema(schema, kvs));
    }

    private static Set<TableReference> getExistingTablesAlsoPresentInSchema(Schema schema, KeyValueService kvs) {
        Set<TableReference> allTables = kvs.getAllTableNames();
        Set<TableReference> schemaFullTableNames = new HashSet<>();

        schemaFullTableNames.addAll(schema.getIndexDefinitions().keySet());
        schemaFullTableNames.addAll(schema.getTableDefinitions().keySet());

        return schemaFullTableNames.stream().filter(allTables::contains).collect(Collectors.toSet());
    }
}
