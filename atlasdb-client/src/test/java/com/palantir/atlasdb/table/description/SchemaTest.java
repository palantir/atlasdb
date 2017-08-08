/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.common.io.Files;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;

public class SchemaTest {

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    private static final String TEST_PACKAGE = "package";
    private static final String TEST_TABLE_NAME = "TestTable";
    private static final String TEST_PATH = TEST_PACKAGE + "/" + TEST_TABLE_NAME + "Table.java";
    private static final TableReference TABLE_REF = TableReference.createWithEmptyNamespace(TEST_TABLE_NAME);

    @Test
    public void testRendersGuavaOptionalsByDefault() throws IOException {
        Schema schema = new Schema("Table", TEST_PACKAGE, Namespace.DEFAULT_NAMESPACE);
        schema.addTableDefinition("TableName", getSimpleTableDefinition(TABLE_REF));
        schema.renderTables(testFolder.getRoot());
        assertThat(readFileIntoString(testFolder.getRoot(), TEST_PATH),
                allOf(
                        containsString("import com.google.common.base.Optional"),
                        containsString("{@link Optional}"),
                        containsString("Optional.absent")));
    }

    @Test
    public void testRendersGuavaOptionalsWhenRequested() throws IOException {
        Schema schema = new Schema("Table", TEST_PACKAGE, Namespace.DEFAULT_NAMESPACE, OptionalType.GUAVA);
        schema.addTableDefinition("TableName", getSimpleTableDefinition(TABLE_REF));
        schema.renderTables(testFolder.getRoot());
        assertThat(readFileIntoString(testFolder.getRoot(), TEST_PATH),
                allOf(
                        containsString("import com.google.common.base.Optional"),
                        not(containsString("import java.util.Optional"))));
    }

    @Test
    public void testRenderJava8OptionalsWhenRequested() throws IOException {
        Schema schema = new Schema("Table", TEST_PACKAGE, Namespace.DEFAULT_NAMESPACE, OptionalType.JAVA8);
        schema.addTableDefinition("TableName", getSimpleTableDefinition(TABLE_REF));
        schema.renderTables(testFolder.getRoot());
        assertThat(readFileIntoString(testFolder.getRoot(), TEST_PATH),
                allOf(
                        not(containsString("import com.google.common.base.Optional")),
                        containsString("import java.util.Optional")));
    }

    @Test
    public void testIgnoreTableNameLengthFlag() throws IOException {
        Schema schema = new Schema("Table", TEST_PACKAGE, Namespace.EMPTY_NAMESPACE);
        schema.ignoreTableNameLengthChecks();
        String longTableName = String.join("", Collections.nCopies(1000, "x"));
        TableReference tableRef = TableReference.createWithEmptyNamespace(longTableName);
        schema.addTableDefinition(longTableName, getSimpleTableDefinition(tableRef));
    }

    @Test
    public void testLongTableNameLengthFailsCassandra() throws IOException {
        Schema schema = new Schema("Table", TEST_PACKAGE, Namespace.EMPTY_NAMESPACE);
        int longLengthCassandra = AtlasDbConstants.CASSANDRA_TABLE_NAME_CHAR_LIMIT + 1;
        String longTableName = String.join("", Collections.nCopies(longLengthCassandra, "x"));
        TableReference tableRef = TableReference.createWithEmptyNamespace(longTableName);
        assertThatThrownBy(() ->
                schema.addTableDefinition(longTableName, getSimpleTableDefinition(tableRef)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Internal table name %s is too long, exceeds Cassandra limit (%s). " +
                        "If using a table prefix, please ensure that the concatenation " +
                        "of the prefix with the internal table name is below the KVS limit. " +
                        "If running using a different KVS, set the ignoreTableNameLength flag.",
                    longTableName,
                    AtlasDbConstants.CASSANDRA_TABLE_NAME_CHAR_LIMIT);
    }

    @Test
    public void testLongTableNameLengthFailsPostgres() throws IOException {
        Schema schema = new Schema("Table", TEST_PACKAGE, Namespace.EMPTY_NAMESPACE);
        int longLengthPostgres = AtlasDbConstants.POSTGRES_TABLE_NAME_CHAR_LIMIT + 1;
        String longTableName = String.join("", Collections.nCopies(longLengthPostgres, "x"));
        TableReference tableRef = TableReference.createWithEmptyNamespace(longTableName);
        assertThatThrownBy(() ->
                schema.addTableDefinition(longTableName, getSimpleTableDefinition(tableRef)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Internal table name %s is too long, exceeds Cassandra limit (%s) and " +
                                "Postgres limit (%s). If using a table prefix, please ensure that the concatenation " +
                                "of the prefix with the internal table name is below the KVS limit. " +
                                "If running using a different KVS, set the ignoreTableNameLength flag.",
                        longTableName,
                        AtlasDbConstants.CASSANDRA_TABLE_NAME_CHAR_LIMIT,
                        AtlasDbConstants.POSTGRES_TABLE_NAME_CHAR_LIMIT);
    }

    @Test
    public void testLongTableNameLengthFailsNamespace() throws IOException {
        // If namespace is non-empty, internal table name length is |namespace| + |tableName| + 2
        Schema schema = new Schema("Table", TEST_PACKAGE, Namespace.DEFAULT_NAMESPACE);
        String longTableName = String.join("", Collections.nCopies(40, "x"));
        TableReference tableRef = TableReference.create(Namespace.DEFAULT_NAMESPACE, longTableName);
        assertThatThrownBy(() ->
                schema.addTableDefinition(longTableName, getSimpleTableDefinition(tableRef)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Internal table name %s is too long, exceeds Cassandra limit (%s). " +
                                "If using a table prefix, please ensure that the concatenation " +
                                "of the prefix with the internal table name is below the KVS limit. " +
                                "If running using a different KVS, set the ignoreTableNameLength flag.",
                        "default__" + longTableName,
                        AtlasDbConstants.CASSANDRA_TABLE_NAME_CHAR_LIMIT);
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
