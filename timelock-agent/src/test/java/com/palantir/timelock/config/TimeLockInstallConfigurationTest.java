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

package com.palantir.timelock.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableList;
import com.palantir.conjure.java.api.config.service.PartialServiceConfiguration;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.timelock.config.PaxosInstallConfiguration.PaxosLeaderMode;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Optional;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TimeLockInstallConfigurationTest {
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

    private File newPaxosLogDirectory;
    private File newSqliteLogDirectory;
    private File extantPaxosLogDirectory;
    private File extantSqliteLogDirectory;

    private PaxosInstallConfiguration newService;
    private PaxosInstallConfiguration extantService;

    @Before
    public void setUp() throws IOException {
        newPaxosLogDirectory = Paths.get(temporaryFolder.getRoot().toString(), "part-time-parliament")
                .toFile();
        newSqliteLogDirectory = Paths.get(temporaryFolder.getRoot().toString(), "sqlite-is-cool")
                .toFile();

        extantPaxosLogDirectory = temporaryFolder.newFolder("lets-do-some-voting");
        extantSqliteLogDirectory = temporaryFolder.newFolder("whats-a-right-join");
    }

    @Test
    public void newServiceIfNewServiceFlagSetToTrue() {
        assertThat(ImmutableTimeLockInstallConfiguration.builder()
                        .cluster(CLUSTER_CONFIG)
                        .paxos(createPaxosInstall(true, false))
                        .build()
                        .isNewServiceNode())
                .isTrue();
    }

    @Test
    public void existingServiceIfNewServiceFlagSetToFalse() {
        assertThat(ImmutableTimeLockInstallConfiguration.builder()
                        .cluster(CLUSTER_CONFIG)
                        .paxos(createPaxosInstall(false, true))
                        .build()
                        .isNewServiceNode())
                .isFalse();
    }

    @Test
    public void newNodeInExistingServiceRecognisedAsNew() {
        assertThat(ImmutableTimeLockInstallConfiguration.builder()
                        .cluster(ImmutableDefaultClusterConfiguration.builder()
                                .localServer(SERVER_A)
                                .cluster(PartialServiceConfiguration.of(
                                        ImmutableList.of(SERVER_A, SERVER_B, "hoof-moved-my-cheese:4567"),
                                        Optional.empty()))
                                .addKnownNewServers(SERVER_A)
                                .build())
                        .paxos(createPaxosInstall(false, false))
                        .build()
                        .isNewServiceNode())
                .isTrue();
    }

    @Test
    public void newServiceNotSetNoDataDirectoryThrows() {
        assertThatThrownBy(() -> ImmutableTimeLockInstallConfiguration.builder()
                .cluster(CLUSTER_CONFIG)
                .paxos(createPaxosInstall(false, false))
                .build())
                .isInstanceOf(SafeIllegalArgumentException.class);
    }

    @Test
    public void newServiceSetNoDataDirectoryExistsThrows() {
        assertThatThrownBy(() -> ImmutableTimeLockInstallConfiguration.builder()
                .cluster(CLUSTER_CONFIG)
                .paxos(createPaxosInstall(true, true))
                .build())
                .isInstanceOf(SafeIllegalArgumentException.class);
    }

    @Test
    public void newServiceNotSetNoDataDirectoryDoesNotThrowWhenIgnoreFlagSet() {
        assertThatCode(() -> ImmutableTimeLockInstallConfiguration.builder()
                .cluster(CLUSTER_CONFIG)
                .paxos(createPaxosInstall(false, false, true))
                .build())
                .doesNotThrowAnyException();
    }

    @Test
    public void newServiceSetNoDataDirectoryExistsDoesNotThrowWhenIgnoreFlagSet() {
        assertThatCode(() -> ImmutableTimeLockInstallConfiguration.builder()
                .cluster(CLUSTER_CONFIG)
                .paxos(createPaxosInstall(true, true, true))
                .build())
                .doesNotThrowAnyException();
    }

    private PaxosInstallConfiguration createPaxosInstall(boolean isNewService, boolean shouldDirectoriesExist) {
        return createPaxosInstall(isNewService, shouldDirectoriesExist, false);
    }

    private PaxosInstallConfiguration createPaxosInstall(boolean isNewService, boolean shouldDirectoriesExist,
            boolean ignoreCheck) {
        return ImmutablePaxosInstallConfiguration.builder()
                .dataDirectory(shouldDirectoriesExist ? extantPaxosLogDirectory : newPaxosLogDirectory)
                .sqlitePersistence(ImmutableSqlitePaxosPersistenceConfiguration.builder()
                        .dataDirectory(shouldDirectoriesExist ? extantSqliteLogDirectory : extantPaxosLogDirectory)
                        .build())
                .isNewService(isNewService)
                .leaderMode(PaxosLeaderMode.SINGLE_LEADER)
                .ignoreNewServiceCheck(ignoreCheck)
                .build();
    }
}
