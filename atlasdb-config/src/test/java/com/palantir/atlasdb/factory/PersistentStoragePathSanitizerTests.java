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

import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import java.io.File;
import java.io.IOException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public final class PersistentStoragePathSanitizerTests {
    @TempDir
    public File testFolder ;

    private String testFolderPath;

    @BeforeEach
    public void setUp() {
        testFolderPath = testFolder.getRoot().getAbsolutePath();
    }

    @Test
    public void emptyFolderSanitization() {
        PersistentStoragePathSanitizer.sanitizeStoragePath(testFolderPath);
    }

    @Test
    public void sanitizingFile() throws IOException {
        File file = testFolder.newFile();

        assertThatThrownBy(() -> PersistentStoragePathSanitizer.sanitizeStoragePath(file.getAbsolutePath()))
                .isInstanceOf(SafeIllegalArgumentException.class)
                .hasMessageContaining("has to point to a directory");
    }

    @Test
    public void removesMagicFolder() {
        PersistentStoragePathSanitizer.sanitizeStoragePath(testFolderPath)
                .toFile()
                .mkdir();
        assertThat(testFolder.getRoot().listFiles()).hasSize(1);

        PersistentStoragePathSanitizer.sanitizeStoragePath(testFolderPath);
        assertThat(testFolder.getRoot().listFiles())
                .as("Sanitization should remove the special subfolder.")
                .isEmpty();
    }
}
