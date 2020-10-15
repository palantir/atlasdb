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

package com.palantir.atlasdb.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import org.assertj.core.util.Files;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public final class PersistentStorageConfigTests {
    private static final File CURRENT_WORKING_DIR = Files.currentFolder();

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder(CURRENT_WORKING_DIR);

    @Test
    public void rocksEmptyDirectory() {
        ImmutableRocksDbPersistentStorageConfig.builder()
                .storagePath("test/path")
                .build();
    }

    @Test
    public void rocksPathToFileThrowsAnException() throws IOException {
        Path filePath = testFolder.newFile("testFile").toPath();
        assertThatThrownBy(() ->
                ImmutableRocksDbPersistentStorageConfig.builder()
                        .storagePath(CURRENT_WORKING_DIR.toPath().relativize(filePath).toString())
                        .build())
                .isInstanceOf(SafeIllegalStateException.class)
                .hasMessageContaining("has to point to a directory");
    }

    @Test
    public void testSerialize() throws IOException {
        assertThat(deserializeClassFromFile("rocksdb-config.yml"))
                .isInstanceOf(RocksDbPersistentStorageConfig.class);
    }

    private static PersistentStorageConfig deserializeClassFromFile(String configPath) throws IOException {
        URL configUrl = RocksDbPersistentStorageConfig.class.getClassLoader().getResource(configPath);
        return AtlasDbConfigs.OBJECT_MAPPER
                .readValue(new File(configUrl.getPath()), RocksDbPersistentStorageConfig.class);
    }
}
