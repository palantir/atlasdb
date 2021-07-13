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

package com.palantir.timelock.paxos;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.spi.KeyValueServiceRuntimeConfig;
import com.palantir.conjure.java.api.config.service.PartialServiceConfiguration;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.timelock.config.ClusterConfiguration;
import com.palantir.timelock.config.PaxosInstallConfiguration;
import com.palantir.timelock.config.TsBoundPersisterRuntimeConfiguration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Optional;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TimeLockAgentTest {

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static final String SERVER_A = "horses-for-courses:1234";
    public static final String SERVER_B = "paddock-and-chips:2345";
    private static final ClusterConfiguration CLUSTER_CONFIG = ImmutableDefaultClusterConfiguration.builder()
            .localServer(SERVER_A)
            .cluster(PartialServiceConfiguration.of(
                    ImmutableList.of(SERVER_A, SERVER_B, "the-mane-event:3456"), Optional.empty()))
            .addKnownNewServers(SERVER_B)
            .build();

    private final PersistedSchemaVersion schemaVersion = mock(PersistedSchemaVersion.class);

    private File newPaxosLogDirectory;
    private File extantPaxosLogDirectory;
    private File extantSqliteLogDirectory;

    @Before
    public void setUp() throws IOException {
        newPaxosLogDirectory = Paths.get(temporaryFolder.getRoot().toString(), "part-time-parliament")
                .toFile();

        extantPaxosLogDirectory = temporaryFolder.newFolder("lets-do-some-voting");
        extantSqliteLogDirectory = temporaryFolder.newFolder("whats-a-right-join");
    }

    @Test
    public void throwWhenPersistedSchemaVersionTooLow() {
        when(schemaVersion.getVersion()).thenReturn(TimeLockAgent.SCHEMA_VERSION - 1);
        assertThatThrownBy(() -> TimeLockAgent.verifySchemaVersion(schemaVersion))
                .as("Persisted version lower than current")
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void throwWhenPersistedSchemaVersionTooHigh() {
        when(schemaVersion.getVersion()).thenReturn(TimeLockAgent.SCHEMA_VERSION + 1);
        assertThatThrownBy(() -> TimeLockAgent.verifySchemaVersion(schemaVersion))
                .as("Persisted version higher than current")
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void doNotThrowWhenPersistedSchemaVersionEqual() {
        when(schemaVersion.getVersion()).thenReturn(TimeLockAgent.SCHEMA_VERSION);
        assertThatCode(() -> TimeLockAgent.verifySchemaVersion(schemaVersion))
                .as("Persisted version lower than current")
                .doesNotThrowAnyException();
    }

    @Test
    public void getKeyValueServiceRuntimeConfigThrowsIfConfiguredToNotUseDatabase() {
        assertThatThrownBy(() ->
                        TimeLockAgent.getKeyValueServiceRuntimeConfig(ImmutableTimeLockRuntimeConfiguration.builder()
                                .timestampBoundPersistence(mock(TsBoundPersisterRuntimeConfiguration.class))
                                .build()))
                .isInstanceOf(SafeIllegalStateException.class)
                .hasMessageContaining("Should not initialise DB Timelock with non-database runtime configuration");
    }

    @Test
    public void getKeyValueServiceRuntimeConfigReturnsEmptyIfNotProvided() {
        assertThat(TimeLockAgent.getKeyValueServiceRuntimeConfig(
                        ImmutableTimeLockRuntimeConfiguration.builder().build()))
                .isEmpty();
    }

    @Test
    public void getKeyValueServiceRuntimeConfigPassesThroughConfigIfAppropriate() {
        // Usage of mock here is to avoid introducing a dependency on atlasdb-dbkvs
        KeyValueServiceRuntimeConfig runtimeConfig = mock(KeyValueServiceRuntimeConfig.class);
        when(runtimeConfig.type()).thenReturn("relational");

        assertThat(TimeLockAgent.getKeyValueServiceRuntimeConfig(ImmutableTimeLockRuntimeConfiguration.builder()
                        .timestampBoundPersistence(ImmutableDatabaseTsBoundPersisterRuntimeConfiguration.builder()
                                .keyValueServiceRuntimeConfig(runtimeConfig)
                                .build())
                        .build()))
                .contains(runtimeConfig);
    }

    @Test
    public void newServiceNotSetNoDataDirectoryThrows() {
        assertThatThrownBy(
                        () -> TimeLockAgent.verifyIsNewServiceInvariant(ImmutableTimeLockInstallConfiguration.builder()
                                .cluster(CLUSTER_CONFIG)
                                .paxos(createPaxosInstall(false, false))
                                .build()))
                .isInstanceOf(SafeIllegalArgumentException.class);
    }

    @Test
    public void newServiceSetNoDataDirectoryExistsThrows() {
        assertThatThrownBy(
                        () -> TimeLockAgent.verifyIsNewServiceInvariant(ImmutableTimeLockInstallConfiguration.builder()
                                .cluster(CLUSTER_CONFIG)
                                .paxos(createPaxosInstall(true, true))
                                .build()))
                .isInstanceOf(SafeIllegalArgumentException.class);
    }

    @Test
    public void newServiceNotSetNoDataDirectoryDoesNotThrowWhenIgnoreFlagSet() {
        assertThatCode(() -> TimeLockAgent.verifyIsNewServiceInvariant(ImmutableTimeLockInstallConfiguration.builder()
                        .cluster(CLUSTER_CONFIG)
                        .paxos(createPaxosInstall(false, false, true))
                        .build()))
                .doesNotThrowAnyException();
    }

    @Test
    public void newServiceSetNoDataDirectoryExistsDoesNotThrowWhenIgnoreFlagSet() {
        assertThatCode(() -> TimeLockAgent.verifyIsNewServiceInvariant(ImmutableTimeLockInstallConfiguration.builder()
                        .cluster(CLUSTER_CONFIG)
                        .paxos(createPaxosInstall(true, true, true))
                        .build()))
                .doesNotThrowAnyException();
    }

    private PaxosInstallConfiguration createPaxosInstall(boolean isNewService, boolean shouldDirectoriesExist) {
        return createPaxosInstall(isNewService, shouldDirectoriesExist, false);
    }

    private PaxosInstallConfiguration createPaxosInstall(
            boolean isNewService, boolean shouldDirectoriesExist, boolean ignoreCheck) {
        return ImmutablePaxosInstallConfiguration.builder()
                .dataDirectory(shouldDirectoriesExist ? extantPaxosLogDirectory : newPaxosLogDirectory)
                .sqlitePersistence(ImmutableSqlitePaxosPersistenceConfiguration.builder()
                        .dataDirectory(shouldDirectoriesExist ? extantSqliteLogDirectory : extantPaxosLogDirectory)
                        .build())
                .isNewService(isNewService)
                .leaderMode(PaxosInstallConfiguration.PaxosLeaderMode.SINGLE_LEADER)
                .ignoreNewServiceCheck(ignoreCheck)
                .build();
    }
}
