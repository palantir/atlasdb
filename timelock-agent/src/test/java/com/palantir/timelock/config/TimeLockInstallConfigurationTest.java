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

import com.google.common.collect.ImmutableList;
import com.palantir.conjure.java.api.config.service.PartialServiceConfiguration;
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
                        .paxos(ImmutablePaxosInstallConfiguration.builder()
                                .dataDirectory(newPaxosLogDirectory)
                                .isNewService(true)
                                .leaderMode(PaxosLeaderMode.SINGLE_LEADER)
                                .sqlitePersistence(ImmutableSqlitePaxosPersistenceConfiguration.builder()
                                        .dataDirectory(newSqliteLogDirectory)
                                        .build())
                                .build())
                        .build()
                        .isNewServiceNode())
                .isTrue();
    }

    @Test
    public void existingServiceIfNewServiceFlagSetToFalse() {
        assertThat(ImmutableTimeLockInstallConfiguration.builder()
                        .cluster(CLUSTER_CONFIG)
                        .paxos(ImmutablePaxosInstallConfiguration.builder()
                                .dataDirectory(extantPaxosLogDirectory)
                                .isNewService(false)
                                .leaderMode(PaxosLeaderMode.SINGLE_LEADER)
                                .sqlitePersistence(ImmutableSqlitePaxosPersistenceConfiguration.builder()
                                        .dataDirectory(extantSqliteLogDirectory)
                                        .build())
                                .build())
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
                        .paxos(ImmutablePaxosInstallConfiguration.builder()
                                .dataDirectory(newPaxosLogDirectory)
                                .isNewService(false)
                                .leaderMode(PaxosLeaderMode.SINGLE_LEADER)
                                .sqlitePersistence(ImmutableSqlitePaxosPersistenceConfiguration.builder()
                                        .dataDirectory(newSqliteLogDirectory)
                                        .build())
                                .build())
                        .build()
                        .isNewServiceNode())
                .isTrue();
    }
}
