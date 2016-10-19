/**
 * Copyright 2016 Palantir Technologies
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.palantir.atlasdb.keyvalue.api.Namespace;

public class SchemaHotspottingTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static final String HOTSPOTTING_TABLE = "HotspottingTable";
    private static final String IGNORED_HOTSPOTTING_TABLE = "IgnoredHotspottingTable";
    private static final String HOTSPOTTER_ROW_COMPONENT = "Hotspotter";

    Schema hotspottingSchema = getHotspottingSchema();
    Schema indexHotspottingSchema = getIndexHotspottingSchema();

    private static Schema getHotspottingSchema() {
        Schema suffersFromHotspoting = new Schema("SuffersFromHotspotting", "unused", Namespace.DEFAULT_NAMESPACE);
        suffersFromHotspoting.addTableDefinition(HOTSPOTTING_TABLE, new TableDefinition() {{
            rowName();
                rowComponent(HOTSPOTTER_ROW_COMPONENT, ValueType.VAR_STRING);
            noColumns();
        }});
        return suffersFromHotspoting;
    }

    private static Schema getIgnoredHotspottingSchema() {
        Schema ignoredHotspottingSchema = new Schema("IgnoredHotspotting", "valid.package", Namespace.DEFAULT_NAMESPACE);
        ignoredHotspottingSchema.addTableDefinition(IGNORED_HOTSPOTTING_TABLE, new TableDefinition() {{
            ignoreHotspottingChecks();
            rowName();
                rowComponent(HOTSPOTTER_ROW_COMPONENT, ValueType.VAR_STRING);
            noColumns();
        }});
        return ignoredHotspottingSchema;
    }

    private static Schema getTableFirstRowComponentHashedSchema() {
        Schema tableFirstRowComponentHashed = new Schema("FirstRowComponentHashed", "unused", Namespace.DEFAULT_NAMESPACE);
        tableFirstRowComponentHashed.addTableDefinition(HOTSPOTTING_TABLE, new TableDefinition() {{
            rowName();
                hashFirstRowComponent();
                rowComponent(HOTSPOTTER_ROW_COMPONENT, ValueType.VAR_STRING);
            noColumns();
        }});
        return tableFirstRowComponentHashed;
    }

    private static Schema getIndexHotspottingSchema() {
        Schema suffersFromIndexHotspoting = getIgnoredHotspottingSchema();
        suffersFromIndexHotspoting.addIndexDefinition("hotSpottingIndex", new IndexDefinition(IndexDefinition.IndexType.CELL_REFERENCING) {{
            onTable(IGNORED_HOTSPOTTING_TABLE);
            rowName();
                componentFromRow(HOTSPOTTER_ROW_COMPONENT, ValueType.VAR_STRING);
        }});
        return suffersFromIndexHotspoting;
    }

    private static Schema getIgnoredIndexHotspottingSchema() {
        Schema ignoredIndexHotspottingSchema = getIgnoredHotspottingSchema();
        ignoredIndexHotspottingSchema.addIndexDefinition("ignoreHotSpottingIndex", new IndexDefinition(IndexDefinition.IndexType.CELL_REFERENCING) {{
            ignoreHotspottingChecks();
            onTable(IGNORED_HOTSPOTTING_TABLE);
            rowName();
                componentFromRow(HOTSPOTTER_ROW_COMPONENT, ValueType.VAR_STRING);
        }});
        return ignoredIndexHotspottingSchema;
    }

    private static Schema getIndexFirstRowComponentHashedSchema() {
        Schema indexFirstRowComponentHashed = getIgnoredHotspottingSchema();
        indexFirstRowComponentHashed.addIndexDefinition("firstRowComponentHashedIndex", new IndexDefinition(IndexDefinition.IndexType.CELL_REFERENCING) {{
            onTable(IGNORED_HOTSPOTTING_TABLE);
            rowName();
                hashFirstRowComponent();
                componentFromRow(HOTSPOTTER_ROW_COMPONENT, ValueType.VAR_STRING);
        }});
        return indexFirstRowComponentHashed;
    }

    @Test (expected = IllegalStateException.class)
    public void testHardFailOnValidateOfTableHotspottingSchema() {
        hotspottingSchema.validate();
    }

    @Test (expected = IllegalStateException.class)
    public void testHardFailOnValidateOfIndexHotspottingSchema() {
        indexHotspottingSchema.validate();
    }

    @Test (expected = IllegalStateException.class)
    public void testFailToGenerateTableHotspottingSchema() throws IOException {
        hotspottingSchema.renderTables(new TemporaryFolder().getRoot());
    }

    @Test (expected = IllegalStateException.class)
    public void testFailToGenerateIndexHotspottingSchema() throws IOException {
        indexHotspottingSchema.renderTables(new TemporaryFolder().getRoot());
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

        assertThat(Arrays.asList(srcDir.list()), contains(equalTo("valid")));

        File validDirectory = srcDir.listFiles()[0];
        assertThat(Arrays.asList(validDirectory.list()), contains(equalTo("package")));
        assertThat(Arrays.asList(validDirectory.listFiles()[0].list()), containsInAnyOrder(equalTo("IgnoredHotspottingTableFactory.java"), equalTo("IgnoredHotspottingTableTable.java")));
    }
}