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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class PaxosInstallConfigurationIntegrationTest {
    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void configurationWithoutExplicitPersistenceConfigurationIsFileBacked() {
        File newFolder = getRandomSubdirectory();
        PaxosInstallConfiguration installConfiguration = ImmutablePaxosInstallConfiguration.builder()
                .dataDirectory(newFolder)
                .isNewService(false)
                .build();

        assertThat(installConfiguration.persistence())
                .isInstanceOf(FilePaxosPersistenceConfiguration.class)
                .satisfies(persistence -> assertThat(((FilePaxosPersistenceConfiguration) persistence).dataDirectory())
                        .isEqualTo(newFolder));
    }

    @Test
    public void fileBackedConfigurationMustAgree() {
        assertThatThrownBy(() -> ImmutablePaxosInstallConfiguration.builder()
                .dataDirectory(getRandomSubdirectory())
                .persistence(ImmutableFilePaxosPersistenceConfiguration.builder()
                        .dataDirectory(getRandomSubdirectory())
                        .build())
                .isNewService(false)
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("If using file based persistence, data directories must match");
    }

    @Test
    public void canConfigureFileBackedConfigurationExplicitly() {
        File newFolder = getRandomSubdirectory();

        assertThatCode(() -> ImmutablePaxosInstallConfiguration.builder()
                .dataDirectory(newFolder)
                .persistence(ImmutableFilePaxosPersistenceConfiguration.builder().dataDirectory(newFolder).build())
                .isNewService(false)
                .build())
                .doesNotThrowAnyException();
    }

    @Test
    public void sqliteBackedConfigurationMustNotShareDataDirectory() {
        File newFolder = getRandomSubdirectory();

        assertThatThrownBy(() -> ImmutablePaxosInstallConfiguration.builder()
                .dataDirectory(newFolder)
                .persistence(ImmutableSqlitePaxosPersistenceConfiguration.builder().dataDirectory(newFolder).build())
                .isNewService(false)
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("If using SQLite based persistence")
                .hasMessageContaining("the SQLite data directory must NOT equal the file based directory.");
    }

    @Test
    public void existenceOfEitherDataDirectorySufficesToIndicateServiceIsNotNew() {
        File fileBackedDirectory = getRandomSubdirectory();
        File sqliteBackedDirectory = getRandomSubdirectory();
        File otherFileBackedDirectory = getRandomSubdirectory();

        assertCanCreateConfigWithSqlitePersistence(fileBackedDirectory, sqliteBackedDirectory);
        fileBackedDirectory.delete();
        assertCanCreateConfigWithSqlitePersistence(fileBackedDirectory, sqliteBackedDirectory);
        sqliteBackedDirectory.delete();
        assertCanCreateConfigWithSqlitePersistence(otherFileBackedDirectory, sqliteBackedDirectory);
    }

    @Test
    public void serviceIsNewIfNoDataDirectoriesPresent() {
        ImmutablePaxosInstallConfiguration.Builder partialConfiguration = ImmutablePaxosInstallConfiguration.builder()
                .dataDirectory(temporaryFolder.getRoot().toPath().resolve("file").toFile())
                .persistence(ImmutableSqlitePaxosPersistenceConfiguration.builder()
                        .dataDirectory(temporaryFolder.getRoot().toPath().resolve("sqlite").toFile())
                        .build());
        assertThatCode(() -> partialConfiguration.isNewService(true).build()).doesNotThrowAnyException();
        assertThatThrownBy(() -> partialConfiguration.isNewService(false).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("The timelock data directories do not appear to exist.");
    }

    public void assertCanCreateConfigWithSqlitePersistence(File fileBackedDirectory, File sqliteBackedDirectory) {
        assertThatCode(() -> ImmutablePaxosInstallConfiguration.builder()
                .dataDirectory(fileBackedDirectory)
                .persistence(ImmutableSqlitePaxosPersistenceConfiguration.builder()
                        .dataDirectory(sqliteBackedDirectory)
                        .build())
                .isNewService(false)
                .build())
                .doesNotThrowAnyException();
    }

    private File getRandomSubdirectory() {
        try {
            return temporaryFolder.newFolder();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
