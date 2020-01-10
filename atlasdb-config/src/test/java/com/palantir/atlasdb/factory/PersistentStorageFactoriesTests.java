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

package com.palantir.atlasdb.factory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;

public final class PersistentStorageFactoriesTests {
    public static final String FIRST_SUBFOLDER_ROOT = "first";
    public static final String SECOND_SUBFOLDER_ROOT = "second";
    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    private String testFolderPath;

    @Before
    public void setUp() {
        testFolderPath = testFolder.getRoot().getAbsolutePath();
    }

    @Test
    public void emptyFolderSanitization() {
        PersistentStorageFactories.sanitizeStoragePath(testFolderPath);
    }

    @Test
    public void createsFolderIfNotExists() {
        File file  = new File(testFolderPath, "nonexistent");
        PersistentStorageFactories.sanitizeStoragePath(file.getPath());

        assertThat(file).isDirectory();
    }

    @Test
    public void sanitizingFile() throws IOException {
        File file = testFolder.newFile();

        assertThatThrownBy(() -> PersistentStorageFactories.sanitizeStoragePath(file.getAbsolutePath()))
                .isInstanceOf(SafeIllegalArgumentException.class)
                .hasMessageContaining("has to point to a directory");
    }

    @Test
    public void removesUuidNamedFolder() throws IOException {
        testFolder.newFolder(UUID.randomUUID().toString());

        PersistentStorageFactories.sanitizeStoragePath(testFolderPath);
        assertThat(testFolder.getRoot().listFiles()).isEmpty();
    }

    @Test
    public void doesNotRemoveFiles() throws IOException {
        testFolder.newFile(UUID.randomUUID().toString());
        testFolder.newFile("testFile");

        PersistentStorageFactories.sanitizeStoragePath(testFolderPath);
        assertThat(testFolder.getRoot().listFiles()).hasSize(2);
    }

    @Test
    public void doesNotRemoveNonUuidNamedFolder() throws IOException {
        testFolder.newFolder("testFolder");

        PersistentStorageFactories.sanitizeStoragePath(testFolderPath);
        assertThat(testFolder.getRoot().listFiles()).hasSize(1);
    }

    @Test
    public void preventMultipleSanitizationOfTheSamePath() throws IOException {
        testFolder.newFolder(UUID.randomUUID().toString());
        PersistentStorageFactories.sanitizeStoragePath(testFolderPath);
        assertThat(testFolder.getRoot().listFiles()).isEmpty();

        testFolder.newFolder(UUID.randomUUID().toString());
        PersistentStorageFactories.sanitizeStoragePath(testFolderPath);
        assertThat(testFolder.getRoot().listFiles()).hasSize(1);
    }

    @Test
    public void doesNotPreventSanitizationOfDifferentPaths() throws IOException {
        File firstRoot = testFolder.newFolder(FIRST_SUBFOLDER_ROOT);
        File secondRoot = testFolder.newFolder(SECOND_SUBFOLDER_ROOT);

        testFolder.newFolder(FIRST_SUBFOLDER_ROOT, UUID.randomUUID().toString());
        testFolder.newFolder(SECOND_SUBFOLDER_ROOT, UUID.randomUUID().toString());

        assertThat(firstRoot.listFiles()).hasSize(1);
        assertThat(secondRoot.listFiles()).hasSize(1);

        PersistentStorageFactories.sanitizeStoragePath(firstRoot.getPath());
        PersistentStorageFactories.sanitizeStoragePath(secondRoot.getPath());

        assertThat(firstRoot.listFiles()).isEmpty();
        assertThat(secondRoot.listFiles()).isEmpty();
    }
}
