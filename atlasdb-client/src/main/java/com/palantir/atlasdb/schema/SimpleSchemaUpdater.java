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

import com.palantir.atlasdb.table.description.IndexDefinition;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.common.annotation.Idempotent;

public interface SimpleSchemaUpdater {
    /**
     * Create a new table with the given name and definition.
     */
    @Idempotent
    void addTable(String tableName, TableDefinition definition);

    /**
     * Drop the table with the given name.
     */
    @Idempotent
    void deleteTable(String tableName);

    /**
     * Create an index with the given name and definition. The name of the index must not end in _aidx.
     */
    @Idempotent
    void addIndex(String indexName, IndexDefinition definition);

    /**
     * Drop the index with the given name.
     */
    @Idempotent
    void deleteIndex(String indexName);

    /**
     * If a table definition is modified, use this to update the schema to reflect the change.
     * <p>
     * Note: This does not change the underlying representation of the data in the table. To do
     * that, use copyTable.
     */
    void updateTableMetadata(String tableName, TableDefinition definition);

    /**
     * If an index definition is modified, use this to update the schema to reflect the change.
     * <p>
     * Note: This does not change the underlying representation of the data in the index. To do
     * that, use copyTable.
     */
    void updateIndexMetadata(String indexName, IndexDefinition definition);

    boolean tableExists(String tableName);

    void initializeSchema(AtlasSchema schema);

    void resetTableMetadata(AtlasSchema schema, String tableName);

    void resetIndexMetadata(AtlasSchema schema, String indexName);
}
