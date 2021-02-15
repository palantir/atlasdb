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
package com.palantir.atlasdb.sweep.priority;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

public class SweepPriorityOverrideConfigTest {
    private static final String FULLY_QUALIFIED_TABLE_NAME_1 = "foo.foo";
    private static final String FULLY_QUALIFIED_TABLE_NAME_2 = "bar.bar";

    private static final String NOT_FULLY_QUALIFIED_TABLE_NAME = "abcdefg";

    @Test
    public void configIsEmptyByDefault() {
        SweepPriorityOverrideConfig defaultConfig =
                ImmutableSweepPriorityOverrideConfig.builder().build();

        assertThat(defaultConfig.blacklistTables()).isEmpty();
        assertThat(defaultConfig.priorityTables()).isEmpty();
    }

    @Test
    public void canSpecifyTablesForBlacklistAndPriority() {
        SweepPriorityOverrideConfig config = ImmutableSweepPriorityOverrideConfig.builder()
                .addBlacklistTables(FULLY_QUALIFIED_TABLE_NAME_1)
                .addPriorityTables(FULLY_QUALIFIED_TABLE_NAME_2)
                .build();

        assertThat(config.blacklistTables()).containsExactly(FULLY_QUALIFIED_TABLE_NAME_1);
        assertThat(config.priorityTables()).containsExactly(FULLY_QUALIFIED_TABLE_NAME_2);
    }

    @Test
    public void cannotSpecifySameTableAsBothBlacklistAndPriority() {
        assertThatThrownBy(() -> ImmutableSweepPriorityOverrideConfig.builder()
                        .addPriorityTables(FULLY_QUALIFIED_TABLE_NAME_1)
                        .addBlacklistTables(FULLY_QUALIFIED_TABLE_NAME_1)
                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(FULLY_QUALIFIED_TABLE_NAME_1);
    }

    @Test
    public void cannotSpecifyTableNamesThatAreNotFullyQualified() {
        assertThatThrownBy(() -> ImmutableSweepPriorityOverrideConfig.builder()
                        .addBlacklistTables(NOT_FULLY_QUALIFIED_TABLE_NAME)
                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(NOT_FULLY_QUALIFIED_TABLE_NAME);
    }

    @Test
    public void priorityTablesAsListReturnsSameElementsAsPriorityTables() {
        SweepPriorityOverrideConfig config = ImmutableSweepPriorityOverrideConfig.builder()
                .addPriorityTables(FULLY_QUALIFIED_TABLE_NAME_1, FULLY_QUALIFIED_TABLE_NAME_2)
                .build();

        assertThat(config.priorityTablesAsList()).hasSameElementsAs(config.priorityTables());
    }
}
