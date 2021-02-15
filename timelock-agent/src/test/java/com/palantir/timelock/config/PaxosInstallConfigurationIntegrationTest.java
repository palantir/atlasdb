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

package com.palantir.timelock.config;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableList;
import com.palantir.conjure.java.api.config.service.PartialServiceConfiguration;
import java.io.File;
import java.io.IOException;
import java.util.Optional;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class PaxosInstallConfigurationIntegrationTest {
    private static final String SERVER_A = "a";
    private static final ClusterConfiguration CLUSTER_CONFIG = ImmutableDefaultClusterConfiguration.builder()
            .localServer(SERVER_A)
            .cluster(PartialServiceConfiguration.of(ImmutableList.of(SERVER_A, "b", "c"), Optional.empty()))
            .build();

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private File nonexistentFileDirectory;
    private File nonexistentSqliteDirectory;

    @Before
    public void setUp() {
        nonexistentFileDirectory =
                temporaryFolder.getRoot().toPath().resolve("file").toFile();
        nonexistentSqliteDirectory =
                temporaryFolder.getRoot().toPath().resolve("sqlite").toFile();
    }

    @Test
    public void serviceIsNewIfNoDataDirectoriesPresent() {
        ImmutablePaxosInstallConfiguration.Builder partialConfiguration =
                createPartialConfiguration(nonexistentFileDirectory, nonexistentSqliteDirectory);
        assertIsNewService(partialConfiguration);
    }

    @Test
    public void serviceIsNotNewIfBaseDataDirectoryIsPresent() {
        ImmutablePaxosInstallConfiguration.Builder partialConfiguration =
                createPartialConfiguration(getAndCreateRandomSubdirectory(), nonexistentSqliteDirectory);
        assertIsNotNewService(partialConfiguration);
    }

    @Test
    public void serviceIsNewIfSqliteDataDirectoryIsPresentButNotBaseDirectory() {
        ImmutablePaxosInstallConfiguration.Builder partialConfiguration =
                createPartialConfiguration(nonexistentFileDirectory, getAndCreateRandomSubdirectory());
        assertIsNewService(partialConfiguration);
    }

    @Test
    public void serviceIsNotNewIfBothDataDirectoriesPresent() {
        ImmutablePaxosInstallConfiguration.Builder partialConfiguration =
                createPartialConfiguration(getAndCreateRandomSubdirectory(), getAndCreateRandomSubdirectory());
        assertIsNotNewService(partialConfiguration);
    }

    @Test
    public void canCreateConfigurationWithDirectoriesHavingPrefixesInName() {
        assertThatCode(() -> createPartialConfiguration(
                                getAndCreateSubdirectory(temporaryFolder.getRoot(), "tom"),
                                getAndCreateSubdirectory(temporaryFolder.getRoot(), "tomato"))
                        .isNewService(false)
                        .build())
                .doesNotThrowAnyException();
    }

    @Test
    public void cannotCreateConfigurationWithIdenticalDirectories() {
        File directory = getAndCreateSubdirectory(temporaryFolder.getRoot(), "tomato");
        assertThatThrownBy(() -> createPartialConfiguration(directory, directory)
                        .isNewService(false)
                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("SQLite and file-based data directories must be different!");
    }

    @Test
    public void sqliteDirectoryCannotBeSubdirectoryOfFileBasedDirectory() {
        File parent = getAndCreateSubdirectory(temporaryFolder.getRoot(), "tomato");
        File child = getAndCreateSubdirectory(parent, "tomatina");
        assertThatThrownBy(() -> createPartialConfiguration(parent, child)
                        .isNewService(false)
                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("SQLite data directory can't be a subdirectory of the file-based data directory");
    }

    @Test
    public void fileBasedDirectoryCannotBeSubdirectoryOfSqliteDirectory() {
        File parent = getAndCreateSubdirectory(temporaryFolder.getRoot(), "tomato");
        File child = getAndCreateSubdirectory(parent, "tomatina");
        assertThatThrownBy(() -> createPartialConfiguration(child, parent)
                        .isNewService(false)
                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("File-based data directory can't be a subdirectory of the SQLite data directory");
    }

    private File getAndCreateRandomSubdirectory() {
        try {
            return temporaryFolder.newFolder();
        } catch (IOException e) {
            throw new RuntimeException("Unexpected error when creating a subdirectory", e);
        }
    }

    private static ImmutablePaxosInstallConfiguration.Builder createPartialConfiguration(
            File dataDirectory, File sqliteDataDirectory) {
        return ImmutablePaxosInstallConfiguration.builder()
                .dataDirectory(dataDirectory)
                .sqlitePersistence(ImmutableSqlitePaxosPersistenceConfiguration.builder()
                        .dataDirectory(sqliteDataDirectory)
                        .build());
    }

    private static void assertIsNotNewService(ImmutablePaxosInstallConfiguration.Builder partialConfiguration) {
        assertThatCode(() -> attemptConstructTopLevelConfigWithoutOverrides(
                        partialConfiguration.isNewService(false).build()))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> attemptConstructTopLevelConfigWithoutOverrides(
                        partialConfiguration.isNewService(true).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("This timelock server has been configured as a new stack");
    }

    private void assertIsNewService(ImmutablePaxosInstallConfiguration.Builder partialConfiguration) {
        assertThatCode(() -> attemptConstructTopLevelConfigWithoutOverrides(
                        partialConfiguration.isNewService(true).build()))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> attemptConstructTopLevelConfigWithoutOverrides(
                        partialConfiguration.isNewService(false).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("The timelock data directories do not appear to exist.");
    }

    private static File getAndCreateSubdirectory(File base, String subdirectoryName) {
        File file = base.toPath().resolve(subdirectoryName).toFile();
        if (file.mkdirs()) {
            return file;
        }
        throw new RuntimeException("Unexpected error when creating a subdirectory");
    }

    @SuppressWarnings("CheckReturnValue")
    private static void attemptConstructTopLevelConfigWithoutOverrides(
            PaxosInstallConfiguration paxosInstallConfiguration) {
        ImmutableTimeLockInstallConfiguration.builder()
                .paxos(paxosInstallConfiguration)
                .cluster(CLUSTER_CONFIG)
                .build();
    }
}
