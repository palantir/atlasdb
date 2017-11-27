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

import static com.palantir.atlasdb.AtlasDbConstants.SCHEMA_V2_TABLE_NAME;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang3.StringUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.common.io.Files;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.protos.generated.SchemaMetadataPersistence.CleanupRequirement;
import com.palantir.atlasdb.schema.stream.StreamStoreDefinitionBuilder;
import com.palantir.atlasdb.schema.stream.StreamTableType;

public class SchemaTest {
    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    private static final String TEST_PACKAGE = "package";
    private static final String TEST_TABLE_NAME = "TestTable";
    private static final String TEST_PATH = TEST_PACKAGE + "/" + TEST_TABLE_NAME + "Table.java";
    private static final TableReference TABLE_REF = TableReference.createWithEmptyNamespace(TEST_TABLE_NAME);
    private static final String EXPECTED_FILES_FOLDER_PATH = "src/integrationInput/java";

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
    // and copy the new files to the ${EXPECTED_FILES_FOLDER_PATH} folder.
    public void checkAgainstAccidentalTableAPIChanges() throws IOException {
        // TODO (amarzoca): Add tests for schemas that use more of the rendering features (Triggers, StreamStores, etc)
        Schema schema = ApiTestSchema.getSchema();
        schema.renderTables(testFolder.getRoot());

        List<String> generatedTestTables = ApiTestSchema.getSchema().getAllTables().stream()
                .map(entry -> entry.getTablename() + "Table")
                .collect(Collectors.toList());

        checkIfFilesAreTheSame(generatedTestTables);
    }

    @Test
    public void checkAgainstAccidentalTableV2APIChanges() throws IOException {
        Schema schema = ApiTestSchema.getSchema();
        schema.renderTables(testFolder.getRoot());

        List<String> generatedTestTables = ApiTestSchema.getSchema().getTableDefinitions().values()
                .stream()
                .filter(TableDefinition::hasV2TableEnabled)
                .map(entry -> entry.getJavaTableName() + SCHEMA_V2_TABLE_NAME)
                .collect(Collectors.toList());

        checkIfFilesAreTheSame(generatedTestTables);
    }

    @Test
    public void simpleTablesHaveNotNeededCleanupRequirement() {
        Schema schema = new Schema("Table", TEST_PACKAGE, Namespace.EMPTY_NAMESPACE);
        schema.addTableDefinition(TEST_TABLE_NAME, getSimpleTableDefinition(TABLE_REF));
        assertCleanupRequirementOnTestTableRef(schema, CleanupRequirement.NOT_NEEDED);
    }

    @Test
    public void tablesWithUnspecifiedSafetyCleanupTasksHaveArbitrarySyncCleanupRequirement() {
        Schema schema = new Schema("Table", TEST_PACKAGE, Namespace.EMPTY_NAMESPACE);
        schema.addTableDefinition(TEST_TABLE_NAME, getSimpleTableDefinition(TABLE_REF));
        schema.addCleanupTask(TEST_TABLE_NAME, (tx, cell) -> false);
        assertCleanupRequirementOnTestTableRef(schema, CleanupRequirement.ARBITRARY_SYNC);
    }

    @Test
    public void tablesWithSingleExplicitlyAsyncCleanupTasksHaveArbitraryAsyncCleanupRequirement() {
        Schema schema = new Schema("Table", TEST_PACKAGE, Namespace.EMPTY_NAMESPACE);
        schema.addTableDefinition(TEST_TABLE_NAME, getSimpleTableDefinition(TABLE_REF));
        schema.addCleanupTask(
                TEST_TABLE_NAME,
                () -> (tx, cell) -> false,
                CleanupRequirement.ARBITRARY_ASYNC);
        assertCleanupRequirementOnTestTableRef(schema, CleanupRequirement.ARBITRARY_ASYNC);
    }

    @Test
    public void tablesWithMultipleExplicitlyAsyncCleanupTasksHaveArbitraryAsyncCleanupRequirement() {
        Schema schema = new Schema("Table", TEST_PACKAGE, Namespace.EMPTY_NAMESPACE);
        schema.addTableDefinition(TEST_TABLE_NAME, getSimpleTableDefinition(TABLE_REF));
        IntStream.range(0, 10)
                .forEach(unused -> schema.addCleanupTask(
                        TEST_TABLE_NAME,
                        () -> (tx, cell) -> false,
                        CleanupRequirement.ARBITRARY_ASYNC));
        assertCleanupRequirementOnTestTableRef(schema, CleanupRequirement.ARBITRARY_ASYNC);
    }

    @Test
    public void tablesWithBothSyncAndAsyncCleanupTasksHaveArbitrarySyncCleanupRequirement() {
        Schema schema = new Schema("Table", TEST_PACKAGE, Namespace.EMPTY_NAMESPACE);
        schema.addTableDefinition(TEST_TABLE_NAME, getSimpleTableDefinition(TABLE_REF));
        schema.addCleanupTask(
                TEST_TABLE_NAME,
                () -> (tx, cell) -> false,
                CleanupRequirement.ARBITRARY_SYNC);
        schema.addCleanupTask(
                TEST_TABLE_NAME,
                () -> (tx, cell) -> false,
                CleanupRequirement.ARBITRARY_ASYNC);
        assertCleanupRequirementOnTestTableRef(schema, CleanupRequirement.ARBITRARY_SYNC);
    }

    @Test
    public void throwsWhenSpecifyingCleanupTaskWithNonArbitraryCleanupRequirements() {
        Schema schema = new Schema("Table", TEST_PACKAGE, Namespace.EMPTY_NAMESPACE);
        assertThatThrownBy(() -> schema.addCleanupTask(
                TEST_TABLE_NAME, () -> (tx, cell) -> false, CleanupRequirement.NOT_NEEDED))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> schema.addCleanupTask(
                TEST_TABLE_NAME, () -> (tx, cell) -> false, CleanupRequirement.STREAM_STORE))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void streamStoreTablesCreatedWithCorrectCleanupRequirements() {
        String shortName = "d";
        String longName = "deoxyribonucleic_acid";

        Schema schema = new Schema("Table", TEST_PACKAGE, Namespace.EMPTY_NAMESPACE);
        schema.addStreamStoreDefinition(
                new StreamStoreDefinitionBuilder(shortName, longName, ValueType.VAR_LONG)
                        .hashRowComponents()
                        .build());

        Map<TableReference, CleanupRequirement> cleanupRequirements = schema.getSchemaMetadata()
                .schemaDependentTableMetadata()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey,
                        entry -> entry.getValue().cleanupRequirement()));

        // TODO (jkong): Change if/when we decide to special-case stream stores
        assertThat(cleanupRequirements.get(
                TableReference.create(Namespace.EMPTY_NAMESPACE, StreamTableType.INDEX.getTableName(shortName))))
                .isEqualTo(CleanupRequirement.ARBITRARY_ASYNC);
        assertThat(cleanupRequirements.get(
                TableReference.create(Namespace.EMPTY_NAMESPACE, StreamTableType.METADATA.getTableName(shortName))))
                .isEqualTo(CleanupRequirement.ARBITRARY_ASYNC);

        assertThat(cleanupRequirements.get(
                TableReference.create(Namespace.EMPTY_NAMESPACE, StreamTableType.VALUE.getTableName(shortName))))
                .isEqualTo(CleanupRequirement.NOT_NEEDED);
        assertThat(cleanupRequirements.get(
                TableReference.create(Namespace.EMPTY_NAMESPACE, StreamTableType.HASH.getTableName(shortName))))
                .isEqualTo(CleanupRequirement.NOT_NEEDED);
    }

    private void assertCleanupRequirementOnTestTableRef(Schema schema, CleanupRequirement requirement) {
        assertThat(schema.getSchemaMetadata().schemaDependentTableMetadata().get(TABLE_REF).cleanupRequirement())
                .isEqualTo(requirement);
    }

    private void checkIfFilesAreTheSame(List<String> generatedTestTables) {
        generatedTestTables.forEach(tableName -> {
            String generatedFilePath =
                    String.format("com/palantir/atlasdb/table/description/generated/%s.java", tableName);

            File expectedFile = new File(EXPECTED_FILES_FOLDER_PATH, generatedFilePath);
            File actualFile = new File(testFolder.getRoot(), generatedFilePath);
            assertThat(actualFile).hasSameContentAs(expectedFile);
        });
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
