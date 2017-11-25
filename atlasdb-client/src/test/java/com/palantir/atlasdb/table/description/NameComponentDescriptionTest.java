/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.LogSafety;
import com.palantir.common.annotation.Immutable;

public class NameComponentDescriptionTest {
    private static final String COMPONENT_NAME = "rowComponent";
    private static final ValueType VALUE_TYPE = ValueType.UUID;
    private static final TableMetadataPersistence.ValueByteOrder VALUE_BYTE_ORDER =
            TableMetadataPersistence.ValueByteOrder.ASCENDING;
    private static final UniformRowNamePartitioner UNIFORM_ROW_NAME_PARTITIONER =
            new UniformRowNamePartitioner(VALUE_TYPE);
    private static final ExplicitRowNamePartitioner EXPLICIT_ROW_NAME_PARTITIONER =
            new ExplicitRowNamePartitioner(VALUE_TYPE, ImmutableSet.of());

    private static final NameComponentDescription DEFAULT_UNNAMED_DESCRIPTION = ImmutableNameComponentDescription
            .builder()
            .componentName("name")
            .type(ValueType.BLOB)
            .build();
    private static final NameComponentDescription LOGGABILITY_UNSPECIFIED_DESCRIPTION
            = ImmutableNameComponentDescription.builder()
                    .componentName(COMPONENT_NAME)
                    .type(VALUE_TYPE)
                    .byteOrder(VALUE_BYTE_ORDER)
                    .uniformPartitioner(UNIFORM_ROW_NAME_PARTITIONER)
                    .explicitPartitioner(null)
                    .build();

    private static final NameComponentDescription NAME_LOGGABLE_DESCRIPTION =
            createWithSpecifiedLogSafety(LogSafety.SAFE);
    private static final NameComponentDescription NAME_NOT_LOGGABLE_DESCRIPTION =
            createWithSpecifiedLogSafety(LogSafety.UNSAFE);

    @Test
    public void builderCanCreateNameComponentDescription() {
        NameComponentDescription description = ImmutableNameComponentDescription.builder()
                .componentName(COMPONENT_NAME)
                .type(VALUE_TYPE)
                .byteOrder(VALUE_BYTE_ORDER)
                .uniformPartitioner(UNIFORM_ROW_NAME_PARTITIONER)
                .explicitPartitioner(EXPLICIT_ROW_NAME_PARTITIONER)
                .logSafety(LogSafety.SAFE)
                .build();

        assertThat(description.getComponentName()).isEqualTo(COMPONENT_NAME);
        assertThat(description.getType()).isEqualTo(VALUE_TYPE);
        assertThat(description.getOrder()).isEqualTo(VALUE_BYTE_ORDER);
        assertThat(description.getUniformPartitioner()).isEqualTo(UNIFORM_ROW_NAME_PARTITIONER);
        assertThat(description.getExplicitPartitioner()).isEqualTo(EXPLICIT_ROW_NAME_PARTITIONER);
        assertThat(description.getLogSafety()).isEqualTo(LogSafety.SAFE);
    }

    @Test
    public void builderSetsSaneDefaults() {
        NameComponentDescription description = ImmutableNameComponentDescription.builder()
                .componentName(COMPONENT_NAME)
                .type(VALUE_TYPE)
                .build();

        assertThat(description.getOrder()).isEqualTo(TableMetadataPersistence.ValueByteOrder.ASCENDING);
        assertThat(description.getUniformPartitioner()).isEqualTo(new UniformRowNamePartitioner(VALUE_TYPE));
        assertThat(description.getExplicitPartitioner()).isNull();
        assertThat(description.getLogSafety()).isEqualTo(LogSafety.UNSAFE);
    }

    @Test
    public void builderCanSetUniformPartitionerToNull() {
        NameComponentDescription description = ImmutableNameComponentDescription.builder()
                .componentName(COMPONENT_NAME)
                .type(VALUE_TYPE)
                .uniformPartitioner(null)
                .build();

        assertThat(description.getUniformPartitioner()).isNull();
    }

    @Test
    public void nameIsNotLoggableInDefaultDescription() {
        assertThat(DEFAULT_UNNAMED_DESCRIPTION.getLogSafety()).isEqualTo(LogSafety.UNSAFE);
    }

    @Test
    public void nameIsNotLoggableIfNotSpecified() {
        assertThat(LOGGABILITY_UNSPECIFIED_DESCRIPTION.getLogSafety()).isEqualTo(LogSafety.UNSAFE);
    }

    @Test
    public void nameCanBeSpecifiedToBeLoggable() {
        assertThat(NAME_LOGGABLE_DESCRIPTION.getLogSafety()).isEqualTo(LogSafety.SAFE);
    }

    @Test
    public void nameCanBeSpecifiedToBeNotLoggable() {
        assertThat(NAME_NOT_LOGGABLE_DESCRIPTION.getLogSafety()).isEqualTo(LogSafety.UNSAFE);
    }

    @Test
    public void canSerializeAndDeserializeDefaultDescription() {
        assertCanSerializeAndDeserializeWithSafety(DEFAULT_UNNAMED_DESCRIPTION, LogSafety.UNSAFE);
    }

    @Test
    public void canSerializeAndDeserializeLoggabilityUnspecifiedDescription() {
        assertCanSerializeAndDeserializeWithSafety(LOGGABILITY_UNSPECIFIED_DESCRIPTION, LogSafety.UNSAFE);
    }

    @Test
    public void canSerializeAndDeserializeKeepingLoggability() {
        assertCanSerializeAndDeserializeWithSafety(NAME_LOGGABLE_DESCRIPTION, LogSafety.SAFE);
    }

    @Test
    public void canSerializeAndDeserializeKeepingNonLoggability() {
        assertCanSerializeAndDeserializeWithSafety(NAME_NOT_LOGGABLE_DESCRIPTION, LogSafety.UNSAFE);
    }

    @Test
    public void withPartitionersPreservesLoggabilityOfName() {
        assertThat(NAME_LOGGABLE_DESCRIPTION.withPartitioners().getLogSafety()).isEqualTo(LogSafety.SAFE);
    }

    @Test
    public void withPartitionersPreservesNonLoggabilityOfName() {
        assertThat(LOGGABILITY_UNSPECIFIED_DESCRIPTION.withPartitioners().getLogSafety()).isEqualTo(LogSafety.UNSAFE);
    }

    private static void assertCanSerializeAndDeserializeWithSafety(
            NameComponentDescription componentDescription,
            LogSafety logSafety) {
        TableMetadataPersistence.NameComponentDescription.Builder builder =
                componentDescription.persistToProto();
        assertThat(NameComponentDescription.hydrateFromProto(builder.build()))
                .isEqualTo(componentDescription)
                .matches(description -> description.getLogSafety() == logSafety);
    }

    private static NameComponentDescription createWithSpecifiedLogSafety(LogSafety logSafety) {
        return ImmutableNameComponentDescription.builder()
                .componentName(COMPONENT_NAME)
                .type(VALUE_TYPE)
                .byteOrder(VALUE_BYTE_ORDER)
                .uniformPartitioner(UNIFORM_ROW_NAME_PARTITIONER)
                .logSafety(logSafety)
                .build();
    }
}
