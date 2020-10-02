/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.config;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.TimestampSeries;

public class DbTimestampCreationParametersTest {
    @Test
    public void canCreateValidMultipleSeriesConfigs() {
        assertThatCode(() -> ImmutableDbTimestampCreationParameters.builder()
                .tsBoundSchema(DatabaseTsBoundSchema.MULTIPLE_SERIES)
                .series(TimestampSeries.of("alice"))
                .build())
                .doesNotThrowAnyException();
        assertThatCode(() -> ImmutableDbTimestampCreationParameters.builder()
                .tsBoundSchema(DatabaseTsBoundSchema.MULTIPLE_SERIES)
                .tableReference(TableReference.createFromFullyQualifiedName("abc.defg"))
                .series(TimestampSeries.of("james"))
                .build())
                .doesNotThrowAnyException();
    }

    @Test
    public void canCreateValidSingleSeriesConfig() {
        assertThatCode(() -> ImmutableDbTimestampCreationParameters.builder()
                .tsBoundSchema(DatabaseTsBoundSchema.ONE_SERIES)
                .build())
                .doesNotThrowAnyException();
        assertThatCode(() -> ImmutableDbTimestampCreationParameters.builder()
                .tsBoundSchema(DatabaseTsBoundSchema.ONE_SERIES)
                .tableReference(TableReference.createFromFullyQualifiedName("pqr.stuv"))
                .build())
                .doesNotThrowAnyException();
    }

    @Test
    public void cannotCreateMultipleSeriesConfigWithoutSeries() {
        assertThatThrownBy(() -> ImmutableDbTimestampCreationParameters.builder()
                .tsBoundSchema(DatabaseTsBoundSchema.MULTIPLE_SERIES)
                .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Cannot have a multiple series configuration without a series specified");
    }

    @Test
    public void cannotCreateSingleSeriesConfigWithSeries() {
        assertThatThrownBy(() -> ImmutableDbTimestampCreationParameters.builder()
                .tsBoundSchema(DatabaseTsBoundSchema.ONE_SERIES)
                .series(TimestampSeries.of("tom"))
                .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Cannot have a single series configuration with a series specified");
    }
}
