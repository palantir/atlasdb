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

import com.palantir.SubdirectoryCreator;
import java.io.File;
import java.time.Clock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class PaxosInstallConfigurationIntegrationTest {
    @TempDir
    public File temporaryFolder;

    private File nonexistentFileDirectory;
    private File nonexistentSqliteDirectory;

    @BeforeEach
    public void setUp() {
        nonexistentFileDirectory = temporaryFolder.toPath().resolve("file").toFile();
        nonexistentSqliteDirectory = temporaryFolder.toPath().resolve("sqlite").toFile();
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
                                SubdirectoryCreator.getAndCreateSubdirectory(temporaryFolder, "tom"),
                                SubdirectoryCreator.getAndCreateSubdirectory(temporaryFolder, "tomato"))
                        .isNewService(false)
                        .build())
                .doesNotThrowAnyException();
    }

    @Test
    public void cannotCreateConfigurationWithIdenticalDirectories() {
        File directory = SubdirectoryCreator.getAndCreateSubdirectory(temporaryFolder, "tomato");
        assertThatThrownBy(() -> createPartialConfiguration(directory, directory)
                        .isNewService(false)
                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("SQLite and file-based data directories must be different!");
    }

    @Test
    public void sqliteDirectoryCannotBeSubdirectoryOfFileBasedDirectory() {
        File parent = SubdirectoryCreator.getAndCreateSubdirectory(temporaryFolder, "tomato");
        File child = SubdirectoryCreator.getAndCreateSubdirectory(parent, "tomatina");
        assertThatThrownBy(() -> createPartialConfiguration(parent, child)
                        .isNewService(false)
                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("SQLite data directory can't be a subdirectory of the file-based data directory");
    }

    @Test
    public void fileBasedDirectoryCannotBeSubdirectoryOfSqliteDirectory() {
        File parent = SubdirectoryCreator.getAndCreateSubdirectory(temporaryFolder, "tomato");
        File child = SubdirectoryCreator.getAndCreateSubdirectory(parent, "tomatina");
        assertThatThrownBy(() -> createPartialConfiguration(child, parent)
                        .isNewService(false)
                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("File-based data directory can't be a subdirectory of the SQLite data directory");
    }

    private File getAndCreateRandomSubdirectory() {
        return SubdirectoryCreator.getAndCreateSubdirectory(
                temporaryFolder, Clock.systemUTC().instant().toString());
    }

    private static ImmutablePaxosInstallConfiguration.Builder createPartialConfiguration(
            File dataDirectory, File sqliteDataDirectory) {
        return PaxosInstallConfiguration.builder()
                .dataDirectory(dataDirectory)
                .sqlitePersistence(ImmutableSqlitePaxosPersistenceConfiguration.builder()
                        .dataDirectory(sqliteDataDirectory)
                        .build());
    }

    private static void assertIsNotNewService(ImmutablePaxosInstallConfiguration.Builder partialConfiguration) {
        assertThatCode(() -> checkPersistenceInvariants(
                        partialConfiguration.isNewService(false).build()))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> checkPersistenceInvariants(
                        partialConfiguration.isNewService(true).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("This timelock server has been configured as a new stack");
    }

    private void assertIsNewService(ImmutablePaxosInstallConfiguration.Builder partialConfiguration) {
        assertThatCode(() -> checkPersistenceInvariants(
                        partialConfiguration.isNewService(true).build()))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> checkPersistenceInvariants(
                        partialConfiguration.isNewService(false).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("The timelock data directories do not appear to exist.");
    }

    @SuppressWarnings("CheckReturnValue")
    private static void checkPersistenceInvariants(PaxosInstallConfiguration paxosInstallConfiguration) {
        TimeLockPersistenceInvariants.checkPersistenceConsistentWithState(
                paxosInstallConfiguration.isNewService(), paxosInstallConfiguration.doDataDirectoriesExist());
    }
}
