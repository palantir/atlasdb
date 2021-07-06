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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.palantir.atlasdb.keyvalue.api.Namespace;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

@SuppressWarnings({"checkstyle:Indentation", "checkstyle:RightCurly"})
public class SchemaHotspottingTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static final String SCHEMA_NAME = "TestSchema";
    private static final String INDEX_NAME = "TestIndex";
    private static final String TABLE_NAME = "TestTable";
    private static final String ROW_COMPONENT_NAME = "TestRowComponent";

    private static Schema getHotspottingSchema() {
        Schema suffersFromHotspoting = new Schema(SCHEMA_NAME, "unused", Namespace.DEFAULT_NAMESPACE);
        suffersFromHotspoting.addTableDefinition(TABLE_NAME, new TableDefinition() {
            {
                rowName();
                rowComponent(ROW_COMPONENT_NAME, ValueType.VAR_STRING);
                noColumns();
            }
        });
        return suffersFromHotspoting;
    }

    private static Schema getIgnoredHotspottingSchema() {
        Schema ignoredHotspottingSchema = new Schema(SCHEMA_NAME, "valid.package", Namespace.DEFAULT_NAMESPACE);
        ignoredHotspottingSchema.addTableDefinition(TABLE_NAME, new TableDefinition() {
            {
                ignoreHotspottingChecks();
                rowName();
                rowComponent(ROW_COMPONENT_NAME, ValueType.VAR_STRING);
                noColumns();
            }
        });
        return ignoredHotspottingSchema;
    }

    private static Schema getTableFirstRowComponentHashedSchema() {
        Schema tableFirstRowComponentHashed = new Schema(SCHEMA_NAME, "unused", Namespace.DEFAULT_NAMESPACE);
        tableFirstRowComponentHashed.addTableDefinition(TABLE_NAME, new TableDefinition() {
            {
                rowName();
                hashFirstRowComponent();
                rowComponent(ROW_COMPONENT_NAME, ValueType.VAR_STRING);
                noColumns();
            }
        });
        return tableFirstRowComponentHashed;
    }

    private static Schema getIndexHotspottingSchema() {
        Schema suffersFromIndexHotspoting = getIgnoredHotspottingSchema();
        suffersFromIndexHotspoting.addIndexDefinition(
                INDEX_NAME, new IndexDefinition(IndexDefinition.IndexType.CELL_REFERENCING) {
                    {
                        onTable(TABLE_NAME);
                        rowName();
                        componentFromRow(ROW_COMPONENT_NAME, ValueType.VAR_STRING);
                    }
                });
        return suffersFromIndexHotspoting;
    }

    private static Schema getIgnoredIndexHotspottingSchema() {
        Schema ignoredIndexHotspottingSchema = getIgnoredHotspottingSchema();
        ignoredIndexHotspottingSchema.addIndexDefinition(
                INDEX_NAME, new IndexDefinition(IndexDefinition.IndexType.CELL_REFERENCING) {
                    {
                        ignoreHotspottingChecks();
                        onTable(TABLE_NAME);
                        rowName();
                        componentFromRow(ROW_COMPONENT_NAME, ValueType.VAR_STRING);
                    }
                });
        return ignoredIndexHotspottingSchema;
    }

    private static Schema getIndexFirstRowComponentHashedSchema() {
        Schema indexFirstRowComponentHashed = getIgnoredHotspottingSchema();
        indexFirstRowComponentHashed.addIndexDefinition(
                INDEX_NAME, new IndexDefinition(IndexDefinition.IndexType.CELL_REFERENCING) {
                    {
                        onTable(TABLE_NAME);
                        rowName();
                        hashFirstRowComponent();
                        componentFromRow(ROW_COMPONENT_NAME, ValueType.VAR_STRING);
                    }
                });
        return indexFirstRowComponentHashed;
    }

    @Test
    public void testHardFailOnValidateOfTableHotspottingSchema() {
        assertThatThrownBy(() -> getHotspottingSchema().validate()).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testHardFailOnValidateOfIndexHotspottingSchema() {
        assertThatThrownBy(() -> getIndexHotspottingSchema().validate()).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testFailToGenerateTableHotspottingSchema() throws IOException {
        assertThatThrownBy(() -> getHotspottingSchema().renderTables(new TemporaryFolder().getRoot()))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testFailToGenerateIndexHotspottingSchema() throws IOException {
        assertThatThrownBy(() -> getIndexHotspottingSchema().renderTables(new TemporaryFolder().getRoot()))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testNoFailureWhenTableHotspottingIgnored() {
        getIgnoredHotspottingSchema().validate();
    }

    @Test
    public void testNoFailureWhenIndexHotspottingIgnored() {
        getIgnoredIndexHotspottingSchema().validate();
    }

    @Test
    public void testNoFailureWhenTableFirstRowComponentHashed() {
        getTableFirstRowComponentHashedSchema().validate();
    }

    @Test
    public void testNoFailureWhenIndexFirstRowComponentHashed() {
        getIndexFirstRowComponentHashedSchema().validate();
    }

    @Test
    public void testSuccessfulGenerationWhenTableHotspottingIgnored() throws IOException {
        File srcDir = temporaryFolder.getRoot();
        getIgnoredHotspottingSchema().renderTables(srcDir);
        System.out.println(Arrays.asList(srcDir.list()));

        assertThat(Arrays.asList(srcDir.list())).contains("valid");

        File validDirectory = srcDir.listFiles()[0];
        assertThat(Arrays.asList(validDirectory.list())).contains("package");
        assertThat(Arrays.asList(validDirectory.listFiles()[0].list()))
                .contains(SCHEMA_NAME + "TableFactory.java", TABLE_NAME + "Table.java");
    }
}
