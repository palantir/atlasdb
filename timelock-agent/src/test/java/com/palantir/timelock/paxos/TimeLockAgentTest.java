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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.spi.KeyValueServiceRuntimeConfig;
import com.palantir.conjure.java.api.config.service.PartialServiceConfiguration;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.timelock.config.ClusterConfiguration;
import com.palantir.timelock.config.DefaultClusterConfiguration;
import com.palantir.timelock.config.ImmutableClusterInstallConfiguration;
import com.palantir.timelock.config.ImmutableDatabaseTsBoundPersisterRuntimeConfiguration;
import com.palantir.timelock.config.ImmutableDefaultClusterConfiguration;
import com.palantir.timelock.config.ImmutablePaxosInstallConfiguration;
import com.palantir.timelock.config.ImmutableSqlitePaxosPersistenceConfiguration;
import com.palantir.timelock.config.ImmutableTimeLockRuntimeConfiguration;
import com.palantir.timelock.config.PaxosInstallConfiguration;
import com.palantir.timelock.config.TimeLockInstallConfiguration;
import com.palantir.timelock.config.TsBoundPersisterRuntimeConfiguration;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Optional;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

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
                                .clusterSnapshot(CLUSTER_CONFIG)
                                .build()))
                .isInstanceOf(SafeIllegalStateException.class)
                .hasMessageContaining("Should not initialise DB Timelock with non-database runtime configuration");
    }

    @Test
    public void getKeyValueServiceRuntimeConfigReturnsEmptyIfNotProvided() {
        assertThat(TimeLockAgent.getKeyValueServiceRuntimeConfig(ImmutableTimeLockRuntimeConfiguration.builder()
                        .clusterSnapshot(CLUSTER_CONFIG)
                        .build()))
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
                        .clusterSnapshot(CLUSTER_CONFIG)
                        .build()))
                .contains(runtimeConfig);
    }

    @Test
    public void newServiceNotSetNoDataDirectoryThrows() {
        assertThatThrownBy(() -> TimeLockAgent.verifyIsNewServiceInvariant(
                        TimeLockInstallConfiguration.builder()
                                .paxos(createPaxosInstall(false, false))
                                .build(),
                        CLUSTER_CONFIG))
                .isInstanceOf(SafeIllegalArgumentException.class);
    }

    @Test
    public void newServiceSetNoDataDirectoryExistsThrows() {
        assertThatThrownBy(() -> TimeLockAgent.verifyIsNewServiceInvariant(
                        TimeLockInstallConfiguration.builder()
                                .paxos(createPaxosInstall(true, true))
                                .build(),
                        CLUSTER_CONFIG))
                .isInstanceOf(SafeIllegalArgumentException.class);
    }

    @Test
    public void newServiceNotSetNoDataDirectoryDoesNotThrowWhenIgnoreFlagSet() {
        assertThatCode(() -> TimeLockAgent.verifyIsNewServiceInvariant(
                        TimeLockInstallConfiguration.builder()
                                .paxos(createPaxosInstall(false, false, true))
                                .build(),
                        CLUSTER_CONFIG))
                .doesNotThrowAnyException();
    }

    @Test
    public void newServiceSetNoDataDirectoryExistsDoesNotThrowWhenIgnoreFlagSet() {
        assertThatCode(() -> TimeLockAgent.verifyIsNewServiceInvariant(
                        TimeLockInstallConfiguration.builder()
                                .paxos(createPaxosInstall(true, true, true))
                                .build(),
                        CLUSTER_CONFIG))
                .doesNotThrowAnyException();
    }

    @Test
    public void throwIfStartedWithLessThanThreeServers_DangerousTopologyNotEnabled() {
        DefaultClusterConfiguration dangerousTopologyConfig = ImmutableDefaultClusterConfiguration.builder()
                .localServer(SERVER_A)
                .cluster(PartialServiceConfiguration.of(ImmutableList.of(SERVER_A, SERVER_B), Optional.empty()))
                .build();
        assertThatThrownBy(() -> TimeLockAgent.verifyTopologyOffersHighAvailability(
                        TimeLockInstallConfiguration.builder()
                                .paxos(createPaxosInstall(false, true, false))
                                .build(),
                        dangerousTopologyConfig))
                .isInstanceOf(SafeIllegalArgumentException.class);
    }

    @Test
    public void doNotIfStartedWithLessThanThreeServers_DangerousTopologyEnabledInInstallConfig() {
        DefaultClusterConfiguration dangerousTopologyConfig = ImmutableDefaultClusterConfiguration.builder()
                .localServer(SERVER_A)
                .cluster(PartialServiceConfiguration.of(ImmutableList.of(SERVER_A, SERVER_B), Optional.empty()))
                .build();
        assertThatCode(() -> TimeLockAgent.verifyTopologyOffersHighAvailability(
                        TimeLockInstallConfiguration.builder()
                                .paxos(createPaxosInstall(false, true, false))
                                .cluster(ImmutableClusterInstallConfiguration.builder()
                                        .enableNonstandardAndPossiblyDangerousTopology(true)
                                        .build())
                                .build(),
                        dangerousTopologyConfig))
                .doesNotThrowAnyException();
    }

    @Test
    public void doNotIfStartedWithLessThanThreeServers_DangerousTopologyEnabledInRuntimeConfig() {
        DefaultClusterConfiguration dangerousTopologyConfig = ImmutableDefaultClusterConfiguration.builder()
                .localServer(SERVER_A)
                .cluster(PartialServiceConfiguration.of(ImmutableList.of(SERVER_A, SERVER_B), Optional.empty()))
                .enableNonstandardAndPossiblyDangerousTopology(true)
                .build();
        assertThatCode(() -> TimeLockAgent.verifyTopologyOffersHighAvailability(
                        TimeLockInstallConfiguration.builder()
                                .paxos(createPaxosInstall(false, true, false))
                                .build(),
                        dangerousTopologyConfig))
                .doesNotThrowAnyException();
    }

    @Test
    public void throwsIfConfiguredToBeNewServiceWithExistingDirectory() throws IOException {
        File mockFile = getMockFileWith(true, true);

        assertFailsToVerifyConfiguration(
                PaxosInstallConfiguration.builder().dataDirectory(mockFile).isNewService(true));
    }

    @Test
    public void throwsIfConfiguredToBeExistingServiceWithoutDirectory() throws IOException {
        File mockFile = getMockFileWith(false, true);

        assertFailsToVerifyConfiguration(
                PaxosInstallConfiguration.builder().dataDirectory(mockFile).isNewService(false));
    }

    @Test
    public void newServiceByClusterBootstrapConfigurationFailsIfDirectoryExists() throws IOException {
        File mockFile = getMockFileWith(true, true);

        ClusterConfiguration differentClusterConfig = ImmutableDefaultClusterConfiguration.builder()
                .localServer(SERVER_A)
                .addKnownNewServers(SERVER_A)
                .cluster(PartialServiceConfiguration.of(ImmutableList.of(SERVER_A, "b", "c"), Optional.empty()))
                .build();

        assertThatThrownBy(() -> TimeLockAgent.verifyIsNewServiceInvariant(
                        TimeLockInstallConfiguration.builder()
                                .paxos(PaxosInstallConfiguration.builder()
                                        .dataDirectory(mockFile)
                                        .isNewService(false)
                                        .build())
                                .build(),
                        differentClusterConfig))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private PaxosInstallConfiguration createPaxosInstall(boolean isNewService, boolean shouldDirectoriesExist) {
        return createPaxosInstall(isNewService, shouldDirectoriesExist, false);
    }

    private PaxosInstallConfiguration createPaxosInstall(
            boolean isNewService, boolean shouldDirectoriesExist, boolean ignoreCheck) {
        return PaxosInstallConfiguration.builder()
                .dataDirectory(shouldDirectoriesExist ? extantPaxosLogDirectory : newPaxosLogDirectory)
                .sqlitePersistence(ImmutableSqlitePaxosPersistenceConfiguration.builder()
                        .dataDirectory(shouldDirectoriesExist ? extantSqliteLogDirectory : extantPaxosLogDirectory)
                        .build())
                .isNewService(isNewService)
                .leaderMode(PaxosInstallConfiguration.PaxosLeaderMode.SINGLE_LEADER)
                .ignoreNewServiceCheck(ignoreCheck)
                .build();
    }

    private File getMockFileWith(boolean isDirectory, boolean canCreateDirectory) throws IOException {
        File mockFile = mock(File.class);
        when(mockFile.mkdirs()).thenReturn(canCreateDirectory);
        when(mockFile.isDirectory()).thenReturn(isDirectory);
        when(mockFile.getPath()).thenReturn("var/data/paxos");
        when(mockFile.getCanonicalPath()).thenReturn("/var/data/paxos");
        return mockFile;
    }

    private void assertFailsToVerifyConfiguration(ImmutablePaxosInstallConfiguration.Builder configBuilder) {
        PaxosInstallConfiguration installConfiguration = configBuilder.build();
        assertThatThrownBy(() -> TimeLockAgent.verifyIsNewServiceInvariant(
                        TimeLockInstallConfiguration.builder()
                                .paxos(installConfiguration)
                                .build(),
                        CLUSTER_CONFIG))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
