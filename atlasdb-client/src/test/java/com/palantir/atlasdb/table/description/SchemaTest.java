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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.common.hash.Hashing;
import com.google.common.io.Files;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.atlasdb.table.description.generated.GenericHashFirstComponentTestTable;
import com.palantir.atlasdb.table.description.generated.GenericHashFirstTwoComponentsTestTable;

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
        List<CharacterLimitType> kvsList = new ArrayList<CharacterLimitType>();
        kvsList.add(CharacterLimitType.CASSANDRA);
        assertThatThrownBy(() ->
                schema.addTableDefinition(longTableName, getSimpleTableDefinition(tableRef)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(getErrorMessage(longTableName, kvsList));
    }

    @Test
    public void testLongTableNameLengthFailsBoth() throws IOException {
        Schema schema = new Schema("Table", TEST_PACKAGE, Namespace.EMPTY_NAMESPACE);
        String longTableName = String.join("", Collections.nCopies(1000, "x"));
        TableReference tableRef = TableReference.createWithEmptyNamespace(longTableName);
        List<CharacterLimitType> kvsList = new ArrayList<CharacterLimitType>();
        kvsList.add(CharacterLimitType.CASSANDRA);
        kvsList.add(CharacterLimitType.POSTGRES);
        assertThatThrownBy(() ->
                schema.addTableDefinition(longTableName, getSimpleTableDefinition(tableRef)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(getErrorMessage(longTableName, kvsList));
    }

    @Test
    public void testLongTableNameLengthFailsNamespace() throws IOException {
        // If namespace is non-empty, internal table name length is |namespace| + |tableName| + 2
        Schema schema = new Schema("Table", TEST_PACKAGE, Namespace.DEFAULT_NAMESPACE);
        String longTableName = String.join("", Collections.nCopies(40, "x"));
        TableReference tableRef = TableReference.create(Namespace.DEFAULT_NAMESPACE, longTableName);
        List<CharacterLimitType> kvsList = new ArrayList<CharacterLimitType>();
        kvsList.add(CharacterLimitType.CASSANDRA);
        assertThatThrownBy(() ->
                schema.addTableDefinition(longTableName, getSimpleTableDefinition(tableRef)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(getErrorMessage(longTableName, kvsList));
    }

    @Test
    // If you are intentionally making Table API changes, please manually regenerate the ApiTestSchema
    public void checkAgainstAccidentalTableAPIChanges() throws IOException {
        // TODO (amarzoca): Add tests for schemas that use more of the rendering features (Triggers, StreamStores, etc)
        Schema schema = ApiTestSchema.getSchema();
        schema.renderTables(testFolder.getRoot());

        String generatedTestTableName = "SchemaApiTestTable";
        String generatedFilePath =
                String.format("com/palantir/atlasdb/table/description/generated/%s.java", generatedTestTableName);

        File expectedFile = new File("src/integrationInput/java", generatedFilePath);
        File actualFile = new File(testFolder.getRoot(), generatedFilePath);

        assertThat(actualFile).hasSameContentAs(expectedFile);
    }

    @Test
    public void testHashFirstRowComponent() {
        GenericHashFirstComponentTestTable.GenericHashFirstComponentTestRow testRow =
                GenericHashFirstComponentTestTable.GenericHashFirstComponentTestRow.of(1, "test");

        byte[] component1Bytes = EncodingUtils.encodeUnsignedVarLong(1);
        long hashOfFirstComponent = Hashing.murmur3_128().hashBytes(component1Bytes).asLong();
        byte[] hashOfFirstComponentBytes = PtBytes.toBytes(Long.MIN_VALUE ^ hashOfFirstComponent);
        byte[] component2Bytes = EncodingUtils.encodeVarString("test");

        assertThat(testRow.persistToBytes())
                .isEqualTo(EncodingUtils.add(hashOfFirstComponentBytes, component1Bytes, component2Bytes));
    }

    @Test
    public void testHashFirstTwoRowComponents() {
        GenericHashFirstTwoComponentsTestTable.GenericHashFirstTwoComponentsTestRow testRow =
                GenericHashFirstTwoComponentsTestTable.GenericHashFirstTwoComponentsTestRow.of(1, "test");

        byte[] component1Bytes = EncodingUtils.encodeUnsignedVarLong(1);
        byte[] component2Bytes = EncodingUtils.encodeVarString("test");
        long hashOfFirstTwoComponents =
                Hashing.murmur3_128().hashBytes(EncodingUtils.add(component1Bytes, component2Bytes)).asLong();
        byte[] hashOfComponentsBytes = PtBytes.toBytes(Long.MIN_VALUE ^ hashOfFirstTwoComponents);

        assertThat(testRow.persistToBytes())
                .isEqualTo(EncodingUtils.add(hashOfComponentsBytes, component1Bytes, component2Bytes));
    }

    private String readFileIntoString(File baseDir, String path) throws IOException {
        return new String(Files.toByteArray(new File(baseDir, path)), StandardCharsets.UTF_8);
    }

    @SuppressWarnings({"checkstyle:Indentation", "checkstyle:RightCurly"})
    private TableDefinition getSimpleTableDefinition(TableReference tableRef) {
        return new TableDefinition() {{
            javaTableName(tableRef.getTablename());
            rowName();
            rowComponent("rowName", ValueType.STRING);
            columns();
            column("col1", "1", ValueType.VAR_LONG);
        }};
    }

    private String getErrorMessage(String tableName, List<CharacterLimitType> kvsExceeded) {
        return String.format("Internal table name %s is too long, known to exceed character limits for "
                        + "the following KVS: %s. If using a table prefix, please ensure that the concatenation "
                        + "of the prefix with the internal table name is below the KVS limit. "
                        + "If running only against a different KVS, set the ignoreTableNameLength flag.",
                tableName, StringUtils.join(kvsExceeded, ", "));
    }

}
