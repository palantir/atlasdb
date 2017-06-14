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

public class NamedColumnDescriptionTest {
    private static final String SHORT_NAME = "shortName";
    private static final String LONG_NAME = "longName";
    private static final ColumnValueDescription COLUMN_VALUE_DESCRIPTION =
            ColumnValueDescription.forType(ValueType.VAR_LONG);

    private static final NamedColumnDescription LOGGABILITY_UNSPECIFIED_DESCRIPTION =
            new NamedColumnDescription(SHORT_NAME, LONG_NAME, COLUMN_VALUE_DESCRIPTION);

    private static final NamedColumnDescription NAME_LOGGABLE_DESCRIPTION =
            new NamedColumnDescription(SHORT_NAME, LONG_NAME, COLUMN_VALUE_DESCRIPTION, true);
    private static final NamedColumnDescription NAME_NOT_LOGGABLE_DESCRIPTION =
            new NamedColumnDescription(SHORT_NAME, LONG_NAME, COLUMN_VALUE_DESCRIPTION, false);

    @Test
    public void nameIsNotLoggableByDefault() {
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
    public void canSerializeAndDeserializeLoggabilityUnspecifiedDescription() {
        assertCanSerializeAndDeserializeWithLoggability(LOGGABILITY_UNSPECIFIED_DESCRIPTION, false);
    }

    @Test
    public void canSerializeAndDeserializeKeepingLoggability() {
        assertCanSerializeAndDeserializeWithLoggability(NAME_LOGGABLE_DESCRIPTION, true);
    }

    @Test
    public void canSerializeAndDeserializeKeepingNonLoggability() {
        assertCanSerializeAndDeserializeWithLoggability(NAME_NOT_LOGGABLE_DESCRIPTION, false);
    }

    private static void assertCanSerializeAndDeserializeWithLoggability(
            NamedColumnDescription componentDescription,
            boolean loggable) {
        TableMetadataPersistence.NamedColumnDescription.Builder builder =
                componentDescription.persistToProto();
        assertThat(NamedColumnDescription.hydrateFromProto(builder.build()))
                .isEqualTo(componentDescription)
                .matches(description -> description.isNameLoggable() == loggable);
    }
}
