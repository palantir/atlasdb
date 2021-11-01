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

import com.palantir.timelock.config.PaxosInstallConfiguration.PaxosLeaderMode;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TimeLockInstallConfigurationTest {
    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private File newPaxosLogDirectory;

    private File extantPaxosLogDirectory;
    private File extantSqliteLogDirectory;

    @Before
    public void setUp() throws IOException {
        newPaxosLogDirectory = Paths.get(temporaryFolder.getRoot().toString(), "part-time-parliament")
                .toFile();
        Paths.get(temporaryFolder.getRoot().toString(), "sqlite-is-cool").toFile();

        extantPaxosLogDirectory = temporaryFolder.newFolder("lets-do-some-voting");
        extantSqliteLogDirectory = temporaryFolder.newFolder("whats-a-right-join");
    }

    @Test
    public void newServiceIfNewServiceFlagSetToTrue() {
        assertThat(TimeLockInstallConfiguration.builder()
                        .paxos(createPaxosInstall(true, false))
                        .build()
                        .isNewService())
                .isTrue();
    }

    @Test
    public void existingServiceIfNewServiceFlagSetToFalse() {
        assertThat(TimeLockInstallConfiguration.builder()
                        .paxos(createPaxosInstall(false, true))
                        .build()
                        .isNewService())
                .isFalse();
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
                .leaderMode(PaxosLeaderMode.SINGLE_LEADER)
                .ignoreNewServiceCheck(ignoreCheck)
                .build();
    }
}
