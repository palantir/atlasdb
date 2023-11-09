/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.dbkvs;

import static com.palantir.logsafe.testing.Assertions.assertThatLoggableException;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.palantir.atlasdb.config.AtlasDbConfigs;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class AlterTableMetadataReferenceTest {
    private static final ObjectMapper OBJECT_MAPPER = AtlasDbConfigs.OBJECT_MAPPER;
    public static final TableReference PRESENT_TABLE_REFERENCE = TableReference.create(Namespace.create("test"), "foo");
    public static final String PRESENT_PHYSICAL_TABLE_NAME = "testfoo";
    public static final String MISSING_PHYSICAL_TABLE_NAME = "asdasdasd";
    public static final TableReference MISSING_TABLE_REFERENCE =
            TableReference.create(Namespace.EMPTY_NAMESPACE, "foo");

    @Test
    public void canDeserialiseMixOfTableReferenceAndPhysicalTableName() throws JsonProcessingException {
        String config = "- namespace:\n   name: test\n  tablename: foo\n- physicalTableName: testfoo";
        List<AlterTableMetadataReference> references = OBJECT_MAPPER.readValue(config, new TypeReference<>() {});
        assertThat(references)
                .contains(
                        ImmutableTableReferenceWrapper.of(PRESENT_TABLE_REFERENCE),
                        ImmutablePhysicalTableNameWrapper.of(PRESENT_PHYSICAL_TABLE_NAME));
    }

    @Test
    public void throwsIfFailToDeserialise() throws JsonProcessingException {
        String config = "namespace: test\ntablename: foo";
        assertThatThrownBy(() -> OBJECT_MAPPER.readValue(config, AlterTableMetadataReference.class))
                .rootCause()
                .isInstanceOf(SafeRuntimeException.class)
                .satisfies(exc -> assertThatLoggableException((SafeRuntimeException) exc)
                        .hasLogMessage(
                                "The alterTablesOrMetadataToMatchAndIKnowWhatIAmDoing value is specified incorrectly."
                                        + " Please either specify a physical table name via `physicalTableName:"
                                        + " physicalTableName`, or as a table reference via \n"
                                        + "`namespace:\n"
                                        + "  name: namespace\n"
                                        + "tablename: tableName")
                        .hasExactlyArgs(SafeArg.of("node", Map.of("namespace", "test", "tablename", "foo"))));
    }

    @Test
    public void doesTableReferenceOrPhysicalTableNameMatchReturnsTrueOnlyWhenMatchingTableRefForTableRefConfig() {
        AlterTableMetadataReference alterTableReference = AlterTableMetadataReference.of(
                PRESENT_TABLE_REFERENCE.getNamespace(), PRESENT_TABLE_REFERENCE.getTableName());

        assertThat(alterTableReference.doesTableReferenceOrPhysicalTableNameMatch(
                        PRESENT_TABLE_REFERENCE, MISSING_PHYSICAL_TABLE_NAME))
                .isTrue();

        assertThat(alterTableReference.doesTableReferenceOrPhysicalTableNameMatch(
                        MISSING_TABLE_REFERENCE, MISSING_PHYSICAL_TABLE_NAME))
                .isFalse();
    }

    @Test
    public void
            doesTableReferenceOrPhysicalTableNameMatchReturnsTrueOnlyWhenMatchingPhysicalTableNameForTableNameConfig() {
        AlterTableMetadataReference alterTableReference = AlterTableMetadataReference.of(PRESENT_PHYSICAL_TABLE_NAME);

        assertThat(alterTableReference.doesTableReferenceOrPhysicalTableNameMatch(
                        MISSING_TABLE_REFERENCE, PRESENT_PHYSICAL_TABLE_NAME))
                .isTrue();

        assertThat(alterTableReference.doesTableReferenceOrPhysicalTableNameMatch(
                        MISSING_TABLE_REFERENCE, MISSING_PHYSICAL_TABLE_NAME))
                .isFalse();
    }
}
