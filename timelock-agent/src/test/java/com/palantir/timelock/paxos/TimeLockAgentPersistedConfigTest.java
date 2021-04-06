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

package com.palantir.timelock.paxos;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.palantir.atlasdb.memory.InMemoryAtlasDbConfig;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.paxos.SqliteConnections;
import com.palantir.remoting2.ext.jackson.ObjectMappers;
import com.palantir.timelock.config.ImmutableDatabaseTsBoundPersisterConfiguration;
import com.palantir.timelock.config.ImmutablePaxosTsBoundPersisterConfiguration;
import com.palantir.timelock.config.TsBoundPersisterConfiguration;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TimeLockAgentPersistedConfigTest {
    private static final ObjectMapper OBJECT_MAPPER = ObjectMappers.newServerObjectMapper();
    private static final TsBoundPersisterConfiguration DATABASE_TS_BOUND_PERSISTER_CONFIGURATION =
            ImmutableDatabaseTsBoundPersisterConfiguration.builder()
                    .keyValueServiceConfig(new InMemoryAtlasDbConfig())
                    .build();
    private static final TsBoundPersisterConfiguration PAXOS_TS_BOUND_PERSISTER_CONFIGURATION =
            ImmutablePaxosTsBoundPersisterConfiguration.builder().build();

    static {
        OBJECT_MAPPER.registerSubtypes(InMemoryAtlasDbConfig.class);
    }

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private HikariDataSource dataSource;

    @Before
    public void setup() {
        dataSource = SqliteConnections.getPooledDataSource(tempFolder.getRoot().toPath());
    }

    @Test
    public void initiallyCanPersistPaxosInformation() {
        assertConfigurationDoesNotDisagree(PAXOS_TS_BOUND_PERSISTER_CONFIGURATION, false);
    }

    @Test
    public void initiallyCanPersistDbInformation() {
        assertConfigurationDoesNotDisagree(DATABASE_TS_BOUND_PERSISTER_CONFIGURATION, false);
    }

    @Test
    public void verifiesPersistedPaxosInformation() {
        assertConfigurationDoesNotDisagree(PAXOS_TS_BOUND_PERSISTER_CONFIGURATION, false);
        assertConfigurationDoesNotDisagree(PAXOS_TS_BOUND_PERSISTER_CONFIGURATION, false);
    }

    @Test
    public void verifiesPersistedDbInformation() {
        assertConfigurationDoesNotDisagree(DATABASE_TS_BOUND_PERSISTER_CONFIGURATION, false);
        assertConfigurationDoesNotDisagree(DATABASE_TS_BOUND_PERSISTER_CONFIGURATION, false);
    }

    @Test
    public void cannotChangeConfigurationFromPaxosToDatabaseWithoutReseeding() {
        assertConfigurationDoesNotDisagree(PAXOS_TS_BOUND_PERSISTER_CONFIGURATION, false);
        assertConfigurationDisagrees(DATABASE_TS_BOUND_PERSISTER_CONFIGURATION);
        assertConfigurationDoesNotDisagree(PAXOS_TS_BOUND_PERSISTER_CONFIGURATION, false);
    }

    @Test
    public void cannotChangeConfigurationFromDatabaseToPaxosWithoutReseeding() {
        assertConfigurationDoesNotDisagree(DATABASE_TS_BOUND_PERSISTER_CONFIGURATION, false);
        assertConfigurationDisagrees(PAXOS_TS_BOUND_PERSISTER_CONFIGURATION);
        assertConfigurationDoesNotDisagree(DATABASE_TS_BOUND_PERSISTER_CONFIGURATION, false);
    }

    @Test
    public void canChangeConfigurationFromPaxosToDatabaseWithReseeding() {
        assertConfigurationDoesNotDisagree(PAXOS_TS_BOUND_PERSISTER_CONFIGURATION, false);
        assertConfigurationDoesNotDisagree(DATABASE_TS_BOUND_PERSISTER_CONFIGURATION, true);
        assertConfigurationDoesNotDisagree(DATABASE_TS_BOUND_PERSISTER_CONFIGURATION, false);
    }

    @Test
    public void cannotChangeConfigurationFromDatabaseToPaxosWithReseeding() {
        assertConfigurationDoesNotDisagree(DATABASE_TS_BOUND_PERSISTER_CONFIGURATION, false);
        assertConfigurationDoesNotDisagree(PAXOS_TS_BOUND_PERSISTER_CONFIGURATION, true);
        assertConfigurationDoesNotDisagree(PAXOS_TS_BOUND_PERSISTER_CONFIGURATION, false);
    }

    @Test
    public void unnecessaryReseedsPermitted() {
        assertConfigurationDoesNotDisagree(PAXOS_TS_BOUND_PERSISTER_CONFIGURATION, true);
        assertConfigurationDoesNotDisagree(PAXOS_TS_BOUND_PERSISTER_CONFIGURATION, true);
    }

    private void assertConfigurationDoesNotDisagree(TsBoundPersisterConfiguration userConfiguration, boolean reseed) {
        assertThatCode(() -> TimeLockAgent.verifyTimestampBoundPersisterConfiguration(
                        dataSource, userConfiguration, reseed, OBJECT_MAPPER))
                .doesNotThrowAnyException();
    }

    private void assertConfigurationDisagrees(TsBoundPersisterConfiguration userConfiguration) {
        // We don't need a reseed parameter here as reseeding should never disagree.
        assertThatThrownBy(() -> TimeLockAgent.verifyTimestampBoundPersisterConfiguration(
                        dataSource, userConfiguration, false, OBJECT_MAPPER))
                .isInstanceOf(SafeIllegalStateException.class)
                .hasMessageContaining("Configuration in the SQLite database does not agree with the configuration"
                        + " the user has provided");
    }
}
