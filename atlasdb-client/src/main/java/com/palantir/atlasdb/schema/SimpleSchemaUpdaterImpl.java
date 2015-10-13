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

import java.util.Set;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.table.description.CodeGeneratingIndexDefinition.IndexType;
import com.palantir.atlasdb.table.description.IndexDefinition;
import com.palantir.atlasdb.table.description.Schemas;
import com.palantir.atlasdb.table.description.TableDefinition;

public class SimpleSchemaUpdaterImpl implements SimpleSchemaUpdater {
    private final KeyValueService kvs;
    private final Namespace namespace;

    private SimpleSchemaUpdaterImpl(KeyValueService kvs, Namespace namespace) {
        this.kvs = kvs;
        this.namespace = namespace;
    }

    public static SimpleSchemaUpdater create(KeyValueService kvs, Namespace namespace) {
        return new SimpleSchemaUpdaterImpl(kvs, namespace);
    }

    @Override
    public void addTable(String tableName, TableDefinition definition) {
        updateTableMetadata(tableName, definition);
    }

    @Override
    public void deleteTable(String tableName) {
        deleteTableMetadata(tableName);
    }

    @Override
    public void addIndex(String indexName, IndexDefinition definition) {
        updateIndexMetadata(indexName, definition);
    }

    @Override
    public void deleteIndex(String indexName) {
        deleteIndexMetadata(indexName);
    }

    @Override
    public void updateTableMetadata(String tableName, TableDefinition definition) {
        String fullTableName = Schemas.getFullTableName(tableName, namespace);
        Schemas.createTable(kvs, fullTableName, definition);
    }

    @Override
    public void updateIndexMetadata(String indexName, IndexDefinition definition) {
        String trueIndexName = Schemas.getFullTableName(Schemas.appendIndexSuffix(indexName, definition), namespace);
        Schemas.createIndex(kvs, trueIndexName, definition);
    }

    private void deleteTableMetadata(String tableName) {
        String fullTableName = Schemas.getFullTableName(tableName, namespace);
        kvs.dropTable(fullTableName);
    }

    private void deleteIndexMetadata(final String indexName) {
        Set<String> tableNames = kvs.getAllTableNames();
        for(IndexType type : IndexType.values()) {
            String trueIndexName = Schemas.getFullTableName(indexName + type.getIndexSuffix(), namespace);
            // This should only happen once - enforced by CodeGeneratingSchema.validateIndex()
            if (tableNames.contains(trueIndexName)) {
                kvs.dropTable(trueIndexName);
                return;
            }
        }
    }

    @Override
    public boolean tableExists(String tableName) {
        String fullTableName = Schemas.getFullTableName(tableName, namespace);
        return kvs.getAllTableNames().contains(fullTableName);
    }

    @Override
    public void initializeSchema(final AtlasSchema schema) {
        Preconditions.checkArgument(schema.getNamespace().equals(namespace));
        Schemas.createTablesAndIndexes(schema.getLatestSchema(), kvs);
    }

    @Override
    public void resetTableMetadata(AtlasSchema schema, String tableName) {
        Schemas.createTable(schema.getLatestSchema(), kvs, tableName);
    }

    @Override
    public void resetIndexMetadata(AtlasSchema schema, String indexName) {
        Schemas.createIndex(schema.getLatestSchema(), kvs, indexName);
    }
}
