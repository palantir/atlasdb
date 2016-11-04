/**
 * Copyright 2016 Palantir Technologies
 * <p>
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://opensource.org/licenses/BSD-3-Clause
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.table.description;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.core.AllOf.allOf;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.common.io.Files;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;

public class SchemaTest {

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    private static TableReference TABLE_REF = TableReference.createWithEmptyNamespace("TestTable");

    @Test
    public void testRenderRespectsOptionalsConfiguration() throws IOException {
        Schema guavaOptionalSchema = new Schema("Table", "package", Namespace.DEFAULT_NAMESPACE, false);
        guavaOptionalSchema.addTableDefinition("TableName", getSimpleTableDefinition(TABLE_REF));
        File guavaOptionalRenderFolder = testFolder.newFolder();
        guavaOptionalSchema.renderTables(guavaOptionalRenderFolder);
        assertThat(readFileIntoString(guavaOptionalRenderFolder, "package/TestTableTable.java"),
                allOf(
                        containsString("import com.google.common.base.Optional"),
                        containsString("{@link Optional}"),
                        containsString("Optional.absent")));

        Schema java8OptionalSchema = new Schema("Table", "package", Namespace.DEFAULT_NAMESPACE, true);
        java8OptionalSchema.addTableDefinition("TableName", getSimpleTableDefinition(TABLE_REF));
        File java8OptionalRenderFolder = testFolder.newFolder();
        java8OptionalSchema.renderTables(java8OptionalRenderFolder);
        assertThat(readFileIntoString(java8OptionalRenderFolder, "package/TestTableTable.java"),
                allOf(
                        containsString("import java.util.Optional"),
                        containsString("{@link Optional}"),
                        containsString("Optional.empty")));
    }

    private String readFileIntoString(File baseDir, String path) throws IOException {
        return new String(Files.toByteArray(new File(baseDir, path)), StandardCharsets.UTF_8);
    }

    private TableDefinition getSimpleTableDefinition(TableReference tableRef) {
        return new TableDefinition() {{
            javaTableName(tableRef.getTablename());
            rowName();
            rowComponent("rowName", ValueType.STRING);
            columns();
            column("col1", "1", ValueType.VAR_LONG);
        }};
    }

}
