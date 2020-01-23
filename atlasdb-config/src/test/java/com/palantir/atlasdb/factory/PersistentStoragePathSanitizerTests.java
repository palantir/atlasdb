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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;

public final class PersistentStoragePathSanitizerTests {
    public static final String FIRST_SUBFOLDER_ROOT = "first";
    public static final String SECOND_SUBFOLDER_ROOT = "second";
    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    private String testFolderPath;
    private PersistentStoragePathSanitizer persistentStoragePathSanitizer;

    @Before
    public void setUp() {
        testFolderPath = testFolder.getRoot().getAbsolutePath();
        persistentStoragePathSanitizer = new PersistentStoragePathSanitizer();
    }

    @Test
    public void emptyFolderSanitization() {
        persistentStoragePathSanitizer.sanitizedStoragePath(testFolderPath);
    }

    @Test
    public void sanitizingFile() throws IOException {
        File file = testFolder.newFile();

        assertThatThrownBy(() -> persistentStoragePathSanitizer.sanitizedStoragePath(file.getAbsolutePath()))
                .isInstanceOf(SafeIllegalArgumentException.class)
                .hasMessageContaining("has to point to a directory");
    }

    @Test
    public void removesMagicFolder() {
        new File(testFolderPath, PersistentStoragePathSanitizer.MAGIC_SUFFIX).mkdir();
        persistentStoragePathSanitizer.sanitizedStoragePath(testFolderPath);
        assertThat(testFolder.getRoot().listFiles()).isEmpty();
    }

    @Test
    public void doesNotRemoveFiles() {
        new File(testFolderPath, "test");

        persistentStoragePathSanitizer.sanitizedStoragePath(testFolderPath);
        assertThat(testFolder.getRoot().listFiles()).hasSize(1);
    }

    @Test
    public void preventMultipleSanitizationOfTheSamePath() throws IOException {
        testFolder.newFolder(PersistentStoragePathSanitizer.MAGIC_SUFFIX);
        persistentStoragePathSanitizer.sanitizedStoragePath(testFolderPath);
        assertThat(testFolder.getRoot().listFiles()).isEmpty();

        testFolder.newFolder(PersistentStoragePathSanitizer.MAGIC_SUFFIX);
        persistentStoragePathSanitizer.sanitizedStoragePath(testFolderPath);
        assertThat(testFolder.getRoot().listFiles()).hasSize(1);
    }

    @Test
    public void doesNotPreventSanitizationOfDifferentPaths() throws IOException {
        File firstRoot = testFolder.newFolder(FIRST_SUBFOLDER_ROOT);
        File secondRoot = testFolder.newFolder(SECOND_SUBFOLDER_ROOT);

        testFolder.newFolder(FIRST_SUBFOLDER_ROOT, PersistentStoragePathSanitizer.MAGIC_SUFFIX);
        testFolder.newFolder(SECOND_SUBFOLDER_ROOT, PersistentStoragePathSanitizer.MAGIC_SUFFIX);

        assertThat(firstRoot.listFiles()).hasSize(1);
        assertThat(secondRoot.listFiles()).hasSize(1);

        persistentStoragePathSanitizer.sanitizedStoragePath(firstRoot.getPath());
        persistentStoragePathSanitizer.sanitizedStoragePath(secondRoot.getPath());

        assertThat(firstRoot.listFiles()).isEmpty();
        assertThat(secondRoot.listFiles()).isEmpty();
    }
}
