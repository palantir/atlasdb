// Copyright 2015 Palantir Technologies
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.atlasdb.table.description;

import java.util.Map;

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
                "Index name cannot end with '" + INDEX_SUFFIX + "'.");
        indexName = indexName + definition.getIndexType().getIndexSuffix();
        return indexName;
    }

    public static void createIndex(KeyValueService kvs, String fullIndexName, IndexDefinition definition) {
        createIndices(kvs, ImmutableMap.of(fullIndexName, definition));
    }

    public static void createIndices(KeyValueService kvs, Map<String, IndexDefinition> fullIndexNameToDefinition) {
        Map<String, Integer> fullIndexNameToMaxValueSize = Maps.newHashMapWithExpectedSize(fullIndexNameToDefinition.size());
        Map<String, byte[]> fullIndexNameToMetadata = Maps.newHashMapWithExpectedSize(fullIndexNameToDefinition.size());

        for (String indexName : fullIndexNameToDefinition.keySet()) {
            fullIndexNameToMaxValueSize.put(indexName, fullIndexNameToDefinition.get(indexName).getMaxValueSize());
            fullIndexNameToMetadata.put(indexName, fullIndexNameToDefinition.get(indexName).toIndexMetadata(indexName).getTableMetadata().persistToBytes());
        }

        kvs.createTables(fullIndexNameToMaxValueSize);
        kvs.putMetadataForTables(fullIndexNameToMetadata);
    }

    public static void createTable(KeyValueService kvs, String fullTableName, TableDefinition definition) {
        createTables(kvs, ImmutableMap.of(fullTableName, definition));
    }

    public static void createTables(KeyValueService kvs, Map<String, TableDefinition>  fullTableNameToDefinition) {
        Map<String, Integer> fullTableNameToMaxValueSize = Maps.newHashMapWithExpectedSize(fullTableNameToDefinition.size());
        Map<String, byte[]> fullTableNameToMetadata = Maps.newHashMapWithExpectedSize(fullTableNameToDefinition.size());

        for (String tableName : fullTableNameToDefinition.keySet()) {
            fullTableNameToMaxValueSize.put(tableName, fullTableNameToDefinition.get(tableName).getMaxValueSize());
            fullTableNameToMetadata.put(tableName, fullTableNameToDefinition.get(tableName).toTableMetadata().persistToBytes());
        }

        kvs.createTables(fullTableNameToMaxValueSize);
        kvs.putMetadataForTables(fullTableNameToMetadata);
    }

    public static String getFullTableName(String tableName, Namespace namespace) {
        validateTableName(tableName);
        String namespaceName = namespace.getName();
        // Hacks for schemas that were created before namespaces were created.
        if (namespace == Namespace.EMPTY_NAMESPACE || namespaceName.equals("met") || namespaceName.equals("upgrade")) {
            return tableName;
        } else {
            return namespace.getName() + "." + tableName;
        }
    }

    public static boolean validateTableName(String tableName) {
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
}
