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
package com.palantir.atlasdb.table.description;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.schema.Namespace;

public final class Schemas {
    private static final String INDEX_SUFFIX = "idx";

    public static String appendIndexSuffix(String indexName, IndexDefinition definition) {
        Preconditions.checkArgument(
                !indexName.endsWith(INDEX_SUFFIX),
                "Index name cannot end with '" + INDEX_SUFFIX + "': " + indexName);
        indexName = indexName + definition.getIndexType().getIndexSuffix();
        return indexName;
    }

    public static void createIndex(KeyValueService kvs, String fullIndexName, IndexDefinition definition) {
        createIndices(kvs, ImmutableMap.of(fullIndexName, definition));
    }

    public static void createIndices(KeyValueService kvs, Map<String, IndexDefinition> fullIndexNameToDefinition) {
        Map<String, byte[]> fullIndexNameToMetadata = Maps.newHashMapWithExpectedSize(fullIndexNameToDefinition.size());
        for (Entry<String, IndexDefinition> indexEntry : fullIndexNameToDefinition.entrySet()) {
            fullIndexNameToMetadata.put(indexEntry.getKey(), indexEntry.getValue().toIndexMetadata(indexEntry.getKey()).getTableMetadata().persistToBytes());
        }
        kvs.createTables(fullIndexNameToMetadata);
    }

    public static void createTable(KeyValueService kvs, String fullTableName, TableDefinition definition) {
        createTables(kvs, ImmutableMap.of(fullTableName, definition));
    }

    public static void createTables(KeyValueService kvs, Map<String, TableDefinition>  fullTableNameToDefinition) {
        Map<String, byte[]> fullTableNameToMetadata = Maps.newHashMapWithExpectedSize(fullTableNameToDefinition.size());
        for (Entry<String, TableDefinition> tableEntry : fullTableNameToDefinition.entrySet()) {
            fullTableNameToMetadata.put(tableEntry.getKey(), tableEntry.getValue().toTableMetadata().persistToBytes());
        }
        kvs.createTables(fullTableNameToMetadata);
    }

    public static String getFullTableName(String tableName, Namespace namespace) {
        Preconditions.checkArgument(isTableNameValid(tableName), "%s is not a valid table name", tableName);
        String namespaceName = namespace.getName();
        // Hacks for schemas that were created before namespaces were created.
        if (namespace.isEmptyNamespace() || namespaceName.equals("met")) {
            return tableName;
        } else {
            return namespace.getName() + "." + tableName;
        }
    }

    public static boolean isTableNameValid(String tableName) {
        for (int i = 0; i < tableName.length() ; i++) {
            char c = tableName.charAt(i);
            if (!Character.isLetterOrDigit(c) && c != '_') {
                return false;
            }
        }
        return true;
    }

    private Schemas() {
        //
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

        Map<String, TableDefinition> fullTableNamesToDefinitions = Maps.newHashMapWithExpectedSize(schema.getTableDefinitions().size());
        for (Entry<String, TableDefinition> e : schema.getTableDefinitions().entrySet()) {
            fullTableNamesToDefinitions.put(getFullTableName(e.getKey(), schema.getNamespace()), e.getValue());
        }
        Map<String, IndexDefinition> fullIndexNamesToDefinitions = Maps.newHashMapWithExpectedSize(schema.getIndexDefinitions().size());
        for (Entry<String, IndexDefinition> e : schema.getIndexDefinitions().entrySet()) {
            fullIndexNamesToDefinitions.put(getFullTableName(e.getKey(), schema.getNamespace()), e.getValue());
        }
        createTables(kvs, fullTableNamesToDefinitions);
        createIndices(kvs, fullIndexNamesToDefinitions);
    }

    public static void createTable(Schema schema, KeyValueService kvs, String tableName) {
        TableDefinition definition = schema.getTableDefinition(tableName);
        String fullTableName = getFullTableName(tableName, schema.getNamespace());
        createTable(kvs, fullTableName, definition);
    }

    public static void createIndex(Schema schema, KeyValueService kvs, String indexName) {
        IndexDefinition definition = schema.getIndex(indexName);
        String fullIndexName = getFullTableName(indexName, schema.getNamespace());
        createIndex(kvs, fullIndexName, definition);
    }

    public static void deleteTablesAndIndexes(Schema schema, KeyValueService kvs) {
        schema.validate();
        Set<String> allTables = kvs.getAllTableNames();
        for (String n : schema.getAllTablesAndIndexMetadata().keySet()) {
            if (allTables.contains(n)) {
                kvs.dropTable(n);
            }
        }
    }

    public static void deleteTable(KeyValueService kvs, String tableName) {
        kvs.dropTable(tableName);
    }
}
