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
import com.palantir.atlasdb.config.DbTimestampCreationSetting;
import com.palantir.atlasdb.config.DbTimestampCreationSettings;
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
        assertConsistent(DbTimestampCreationSettings.singleSeries(Optional.empty()));
    }

    @Test
    public void oneSeriesWithDefaultTableSpecifiedIsConsistentWithDefaults() {
        assertConsistent(DbTimestampCreationSettings.singleSeries(Optional.of(AtlasDbConstants.TIMESTAMP_TABLE)));
    }

    @Test
    public void oneSeriesWithDbTimeLockTablesIsInconsistentWithDefaults() {
        assertInconsistent(DbTimestampCreationSettings.singleSeries(
                Optional.of(AtlasDbConstants.LEGACY_TIMELOCK_TIMESTAMP_TABLE)));
        assertInconsistent(DbTimestampCreationSettings.singleSeries(
                Optional.of(AtlasDbConstants.DB_TIMELOCK_TIMESTAMP_TABLE)));
    }

    @Test
    public void oneSeriesWithRandomTablesIsInconsistentWithDefaults() {
        assertInconsistent(DbTimestampCreationSettings.singleSeries(
                Optional.of(TableReference.createFromFullyQualifiedName("abc.tom"))));
    }

    @Test
    public void multipleSeriesIsInconsistentWithDefaults() {
        assertInconsistent(DbTimestampCreationSettings.multipleSeries(
                TableReference.createFromFullyQualifiedName("def.tony"),
                TimestampSeries.of("onetwothreefourfive")));
    }

    private static void assertConsistent(DbTimestampCreationSetting parameters) {
        assertThat(areParametersConsistent(parameters)).isTrue();
    }

    private static boolean areParametersConsistent(DbTimestampCreationSetting parameters) {
        return TimestampCreationParametersCheck.areCreationParametersConsistentWithDefaults(
                Optional.of(parameters));
    }

    private static void assertInconsistent(DbTimestampCreationSetting parameters) {
        assertThat(areParametersConsistent(parameters)).isFalse();
    }
}
