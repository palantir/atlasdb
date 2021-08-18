/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.timelock.store;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.palantir.atlasdb.memory.InMemoryAtlasDbConfig;
import com.palantir.conjure.java.serialization.ObjectMappers;
import com.palantir.paxos.SqliteConnections;
import com.palantir.timelock.config.ImmutableDatabaseTsBoundPersisterConfiguration;
import com.palantir.timelock.config.ImmutablePaxosTsBoundPersisterConfiguration;
import com.palantir.timelock.config.TsBoundPersisterConfiguration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class PersistenceConfigStoreTest {
    private static final TsBoundPersisterConfiguration PAXOS_CONFIG =
            ImmutablePaxosTsBoundPersisterConfiguration.builder().build();
    private static final TsBoundPersisterConfiguration DB_CONFIG =
            ImmutableDatabaseTsBoundPersisterConfiguration.builder()
                    .keyValueServiceConfig(new InMemoryAtlasDbConfig())
                    .build();

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private PersistenceConfigStore persistenceConfigStore;

    @Before
    public void setup() {
        SqliteBlobStore blobStore = SqliteBlobStore.create(SqliteConnections.getDefaultConfiguredPooledDataSource(
                tempFolder.getRoot().toPath()));
        ObjectMapper objectMapper = ObjectMappers.newServerObjectMapper();
        objectMapper.registerSubtypes(InMemoryAtlasDbConfig.class);
        persistenceConfigStore = new PersistenceConfigStore(objectMapper, blobStore);
    }

    @Test
    public void getsEmptyIfNoConfigStored() {
        assertThat(persistenceConfigStore.getPersistedConfig()).isEmpty();
    }

    @Test
    public void canStoreAndRetrieveConfigs() {
        persistenceConfigStore.storeConfig(PAXOS_CONFIG);
        assertThat(persistenceConfigStore.getPersistedConfig()).contains(PAXOS_CONFIG);

        persistenceConfigStore.storeConfig(DB_CONFIG);
        assertThat(persistenceConfigStore.getPersistedConfig()).contains(DB_CONFIG);
    }
}
