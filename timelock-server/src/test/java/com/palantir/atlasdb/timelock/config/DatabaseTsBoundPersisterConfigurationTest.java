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
package com.palantir.atlasdb.timelock.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

import com.palantir.atlasdb.memory.InMemoryAtlasDbConfig;
import com.palantir.timelock.config.DatabaseTsBoundPersisterConfiguration;
import com.palantir.atlasdb.config.DatabaseTsBoundSchema;
import com.palantir.timelock.config.ImmutableDatabaseTsBoundPersisterConfiguration;

public class DatabaseTsBoundPersisterConfigurationTest {
    @Test
    public void canCreateWithInMemoryKvsConfig() {
        DatabaseTsBoundPersisterConfiguration config = ImmutableDatabaseTsBoundPersisterConfiguration.builder()
                .keyValueServiceConfig(new InMemoryAtlasDbConfig())
                .databaseTsBoundSchema(DatabaseTsBoundSchema.MULTIPLE_SERIES)
                .build();
        assertThat(config).isNotNull();
    }

    @Test
    public void mustIndicateDatabaseTsBoundSchemaExplicitly() {
        assertThatThrownBy(() -> ImmutableDatabaseTsBoundPersisterConfiguration.builder()
                .keyValueServiceConfig(new InMemoryAtlasDbConfig())
                .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("some of required attributes are not set");
    }
}
