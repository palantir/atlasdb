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
package com.palantir.atlasdb;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.ValueByteOrder;
import com.palantir.atlasdb.schema.Namespace;
import com.palantir.atlasdb.schema.SimpleSchemaUpdater;
import com.palantir.atlasdb.schema.SimpleSchemaUpdaterImpl;
import com.palantir.atlasdb.table.description.CodeGeneratingIndexDefinition;
import com.palantir.atlasdb.table.description.CodeGeneratingIndexDefinition.IndexType;
import com.palantir.atlasdb.table.description.CodeGeneratingTableDefinition;
import com.palantir.atlasdb.table.description.DefaultTableMetadata;
import com.palantir.atlasdb.table.description.IndexDefinition;
import com.palantir.atlasdb.table.description.NamedColumnDescription;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;

public class SchemaUpdaterTest extends AtlasDbTestCase {

    private SimpleSchemaUpdater updater;

    @Before
    public void makeUpdater() {
        updater = SimpleSchemaUpdaterImpl.create(keyValueService, Namespace.DEFAULT_NAMESPACE);
    }

    @Test
    public void testAddTable() {
        updater.addTable("table", getTableWithOneCol("a"));
        Assert.assertTrue(keyValueService.getAllTableNames().contains("default.table"));
    }

    @Test
    public void testDeleteTable() {
        updater.addTable("table", getTableWithOneCol("a"));
        updater.deleteTable("table");
        Assert.assertFalse(keyValueService.getAllTableNames().contains("default.table"));
    }

    @Test
    public void testAddIndex() {
        updater.addTable("table", getTableWithOneCol("a"));
        updater.addIndex("newIndex", getIndex("table"));
        Assert.assertTrue(keyValueService.getAllTableNames().contains("default.newIndex" + "_aidx"));
    }

    @Test
    public void testDeleteIndex() {
        updater.addTable("table", getTableWithOneCol("a"));
        updater.addIndex("newIndex", getIndex("table"));
        updater.deleteIndex("newIndex");
        Assert.assertFalse(keyValueService.getAllTableNames().contains("default.newIndex" + "_aidx"));
    }

    @Test
    public void testAddColumnToTable() {
        updater.addTable("table", getTableWithOneCol("a"));
        updater.updateTableMetadata("table", getTableWithTwoCols("a", "b"));

        TableMetadata metadata = DefaultTableMetadata.BYTES_HYDRATOR.hydrateFromBytes(keyValueService.getMetadataForTable("table"));
        boolean containsNewColumn = false;
        for(NamedColumnDescription column : metadata.getColumns().getNamedColumns()) {
            if (column.getLongName().equals("b")) {
                containsNewColumn = true;
            }
        }
        Assert.assertTrue(containsNewColumn);
    }

    @Test
    public void testDeleteColumnFromTable() {
        updater.addTable("table", getTableWithTwoCols("a", "b"));
        updater.updateTableMetadata("table", getTableWithOneCol("a"));

        TableMetadata metadata = DefaultTableMetadata.BYTES_HYDRATOR.hydrateFromBytes(keyValueService.getMetadataForTable("table"));

        for(NamedColumnDescription column : metadata.getColumns().getNamedColumns()) {
            Assert.assertFalse(column.getLongName().equals("b"));
        }
    }

    private IndexDefinition getIndex(final String tableName) {
        return new CodeGeneratingIndexDefinition(IndexType.ADDITIVE) {{
            onTable(tableName);
            rowName();
                componentFromRow("row_id", ValueType.FIXED_LONG,
                        ValueByteOrder.DESCENDING, "id");
        }};
    }

    private TableDefinition getTableWithTwoCols(final String col1, final String col2) {
        return new CodeGeneratingTableDefinition() {{
            rowName();
                rowComponent("id", ValueType.VAR_LONG);
                columns();
                column(col1, col1, ValueType.BLOB);
                column(col2, col2, ValueType.BLOB);
                maxValueSize(128);
            conflictHandler(ConflictHandler.IGNORE_ALL);
        }};
    }

    private TableDefinition getTableWithOneCol(final String col) {
        return new CodeGeneratingTableDefinition() {{
            rowName();
                rowComponent("id", ValueType.VAR_LONG);
            columns();
                column(col, col, ValueType.BLOB);
            maxValueSize(128);
            conflictHandler(ConflictHandler.RETRY_ON_WRITE_WRITE);
        }};
    }
}
