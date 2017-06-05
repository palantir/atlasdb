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

import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;

public class NameComponentDescriptionTest {
    private static final String COMPONENT_NAME = "rowComponent";
    private static final ValueType VALUE_TYPE = ValueType.UUID;
    private static final TableMetadataPersistence.ValueByteOrder VALUE_BYTE_ORDER =
            TableMetadataPersistence.ValueByteOrder.ASCENDING;
    private static final UniformRowNamePartitioner UNIFORM_ROW_NAME_PARTITIONER =
            new UniformRowNamePartitioner(VALUE_TYPE);

    private static final NameComponentDescription DEFAULT_UNNAMED_DESCRIPTION = new NameComponentDescription();
    private static final NameComponentDescription LOGGABILITY_UNSPECIFIED_DESCRIPTION =
            new NameComponentDescription(
                    COMPONENT_NAME,
                    VALUE_TYPE,
                    VALUE_BYTE_ORDER,
                    UNIFORM_ROW_NAME_PARTITIONER,
                    null);

    private static final NameComponentDescription NAME_LOGGABLE_DESCRIPTION =
            createWithSpecifiedLoggability(true);
    private static final NameComponentDescription NAME_NOT_LOGGABLE_DESCRIPTION =
            createWithSpecifiedLoggability(false);

    @Test
    public void nameIsNotLoggableInDefaultDescription() {
        assertThat(DEFAULT_UNNAMED_DESCRIPTION.isNameLoggable()).isFalse();
    }

    @Test
    public void nameIsNotLoggableIfNotSpecified() {
        assertThat(LOGGABILITY_UNSPECIFIED_DESCRIPTION.isNameLoggable()).isFalse();
    }

    @Test
    public void nameCanBeSpecifiedToBeLoggable() {
        assertThat(NAME_LOGGABLE_DESCRIPTION.isNameLoggable()).isTrue();
    }

    @Test
    public void nameCanBeSpecifiedToBeNotLoggable() {
        assertThat(NAME_NOT_LOGGABLE_DESCRIPTION.isNameLoggable()).isFalse();
    }

    @Test
    public void canSerializeAndDeserializeKeepingLoggability() {
        TableMetadataPersistence.NameComponentDescription.Builder builder = NAME_LOGGABLE_DESCRIPTION.persistToProto();
        assertThat(NameComponentDescription.hydrateFromProto(builder.build()))
                .isEqualTo(NAME_LOGGABLE_DESCRIPTION)
                .matches(NameComponentDescription::isNameLoggable);
    }

    @Test
    public void canSerializeAndDeserializeKeepingNonLoggability() {
        TableMetadataPersistence.NameComponentDescription.Builder builder =
                NAME_NOT_LOGGABLE_DESCRIPTION.persistToProto();
        assertThat(NameComponentDescription.hydrateFromProto(builder.build()))
                .isEqualTo(NAME_NOT_LOGGABLE_DESCRIPTION)
                .matches(description -> !description.isNameLoggable());
    }

    @Test
    public void withPartitionersPreservesLoggabilityOfName() {
        assertThat(NAME_LOGGABLE_DESCRIPTION.withPartitioners().isNameLoggable()).isTrue();
    }

    @Test
    public void withPartitionersPreservesNonLoggabilityOfName() {
        assertThat(LOGGABILITY_UNSPECIFIED_DESCRIPTION.withPartitioners().isNameLoggable()).isFalse();
    }

    private static NameComponentDescription createWithSpecifiedLoggability(boolean loggable) {
        return new NameComponentDescription(
                COMPONENT_NAME,
                VALUE_TYPE,
                VALUE_BYTE_ORDER,
                UNIFORM_ROW_NAME_PARTITIONER,
                null,
                loggable);
    }
}
