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

    Schema hotspottingSchema = getHotspottingSchema();

    private static Schema getHotspottingSchema() {
        Schema suffersFromHotspoting = new Schema("SuffersFromHotspoting", "unused", Namespace.DEFAULT_NAMESPACE);
        suffersFromHotspoting.addTableDefinition("HotspottingTable", new TableDefinition() {{
            rowName();
                rowComponent("Hotspotter", ValueType.VAR_STRING);
            noColumns();
        }});
        return suffersFromHotspoting;
    }

    private static Schema getIgnoredHotspottingSchema() {
        Schema ignoredHotspotting = new Schema("IgnoredHotspotting", "valid.package", Namespace.DEFAULT_NAMESPACE);
        ignoredHotspotting.addTableDefinition("IgnoredHotspottingTable", new TableDefinition() {{
            ignoreHotspottingChecks();
            rowName();
                rowComponent("Hotspotter", ValueType.VAR_STRING);
            noColumns();
        }});
        return ignoredHotspotting;
    }

    @Test (expected = IllegalStateException.class)
    public void testHardFailOnValidateOfHotspottingSchema() {
        hotspottingSchema.validate();
    }

    @Test (expected = IllegalStateException.class)
    public void testFailToGenerateHotspottingSchema() throws IOException {
        hotspottingSchema.renderTables(new TemporaryFolder().getRoot());
    }

    @Test
    public void testNoFailureWhenHotspottingIgnored() {
        getIgnoredHotspottingSchema().validate();
    }

    @Test
    public void testSuccessfulGenerationWhenHotspottingIgnored() throws IOException {
        File srcDir = temporaryFolder.getRoot();
        getIgnoredHotspottingSchema().renderTables(srcDir);

        assertThat(Arrays.asList(srcDir.list()), contains(equalTo("valid")));

        File validDirectory = srcDir.listFiles()[0];
        assertThat(Arrays.asList(validDirectory.list()), contains(equalTo("package")));
        assertThat(Arrays.asList(validDirectory.listFiles()[0].list()), contains(equalTo("IgnoredHotspottingTableFactory.java"), equalTo("IgnoredHotspottingTableTable.java")));
    }
}