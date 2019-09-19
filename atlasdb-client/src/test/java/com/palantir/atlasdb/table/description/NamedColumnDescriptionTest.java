/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.table.description;

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.LogSafety;
import org.junit.Test;

public class NamedColumnDescriptionTest {
    private static final String SHORT_NAME = "shortName";
    private static final String LONG_NAME = "longName";
    private static final ColumnValueDescription COLUMN_VALUE_DESCRIPTION =
            ColumnValueDescription.forType(ValueType.VAR_LONG);

    private static final NamedColumnDescription LOGGABILITY_UNSPECIFIED_DESCRIPTION =
            new NamedColumnDescription(SHORT_NAME, LONG_NAME, COLUMN_VALUE_DESCRIPTION);

    private static final NamedColumnDescription NAME_LOGGABLE_DESCRIPTION =
            new NamedColumnDescription(SHORT_NAME, LONG_NAME, COLUMN_VALUE_DESCRIPTION, LogSafety.SAFE);
    private static final NamedColumnDescription NAME_NOT_LOGGABLE_DESCRIPTION =
            new NamedColumnDescription(SHORT_NAME, LONG_NAME, COLUMN_VALUE_DESCRIPTION, LogSafety.UNSAFE);

    @Test
    public void nameIsNotLoggableByDefault() {
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

    private static void assertCanSerializeAndDeserializeWithSafety(
            NamedColumnDescription componentDescription,
            LogSafety logSafety) {
        TableMetadataPersistence.NamedColumnDescription.Builder builder =
                componentDescription.persistToProto();
        assertThat(NamedColumnDescription.hydrateFromProto(builder.build()))
                .isEqualTo(componentDescription)
                .matches(description -> description.logSafety == logSafety);
    }
}
