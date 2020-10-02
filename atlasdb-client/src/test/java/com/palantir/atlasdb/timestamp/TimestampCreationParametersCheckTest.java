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

package com.palantir.atlasdb.timestamp;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;

import org.junit.Test;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.config.DatabaseTsBoundSchema;
import com.palantir.atlasdb.config.DbTimestampCreationParameters;
import com.palantir.atlasdb.config.ImmutableDbTimestampCreationParameters;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.TimestampSeries;

public class TimestampCreationParametersCheckTest {
    @Test
    public void emptyIsConsistentWithDefaults() {
        assertThat(TimestampCreationParametersCheck.areCreationParametersConsistentWithDefaults(Optional.empty()))
                .isTrue();
    }

    @Test
    public void oneSeriesWithNoTableSpecifiedIsConsistentWithDefaults() {
        assertConsistent(ImmutableDbTimestampCreationParameters.builder()
                .tsBoundSchema(DatabaseTsBoundSchema.ONE_SERIES)
                .build());
    }

    @Test
    public void oneSeriesWithDefaultTableSpecifiedIsConsistentWithDefaults() {
        assertConsistent(ImmutableDbTimestampCreationParameters.builder()
                .tsBoundSchema(DatabaseTsBoundSchema.ONE_SERIES)
                .tableReference(AtlasDbConstants.TIMESTAMP_TABLE)
                .build());
    }

    @Test
    public void oneSeriesWithDbTimeLockTablesIsInconsistentWithDefaults() {
        assertInconsistent(ImmutableDbTimestampCreationParameters.builder()
                .tsBoundSchema(DatabaseTsBoundSchema.ONE_SERIES)
                .tableReference(AtlasDbConstants.LEGACY_TIMELOCK_TIMESTAMP_TABLE)
                .build());
        assertInconsistent(ImmutableDbTimestampCreationParameters.builder()
                .tsBoundSchema(DatabaseTsBoundSchema.ONE_SERIES)
                .tableReference(AtlasDbConstants.DB_TIMELOCK_TIMESTAMP_TABLE)
                .build());
    }

    @Test
    public void oneSeriesWithRandomTablesIsInconsistentWithDefaults() {
        assertInconsistent(ImmutableDbTimestampCreationParameters.builder()
                .tsBoundSchema(DatabaseTsBoundSchema.ONE_SERIES)
                .tableReference(TableReference.createFromFullyQualifiedName("abc.tom"))
                .build());
    }

    @Test
    public void multipleSeriesIsInconsistentWithDefaults() {
        assertInconsistent(ImmutableDbTimestampCreationParameters.builder()
                .tsBoundSchema(DatabaseTsBoundSchema.MULTIPLE_SERIES)
                .series(TimestampSeries.of("einszweidreivierfünf"))
                .build());
        assertInconsistent(ImmutableDbTimestampCreationParameters.builder()
                .tsBoundSchema(DatabaseTsBoundSchema.MULTIPLE_SERIES)
                .series(TimestampSeries.of("onetwothreefourfive"))
                .tableReference(TableReference.createFromFullyQualifiedName("def.tony"))
                .build());
    }

    private static void assertConsistent(DbTimestampCreationParameters parameters) {
        assertThat(areParametersConsistent(parameters)).isTrue();
    }

    private static boolean areParametersConsistent(DbTimestampCreationParameters parameters) {
        return TimestampCreationParametersCheck.areCreationParametersConsistentWithDefaults(
                Optional.of(parameters));
    }

    private static void assertInconsistent(DbTimestampCreationParameters parameters) {
        assertThat(areParametersConsistent(parameters)).isFalse();
    }
}