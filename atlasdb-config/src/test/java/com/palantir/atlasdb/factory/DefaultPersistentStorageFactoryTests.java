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

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.config.ImmutableRocksDbPersistentStorageConfig;
import com.palantir.atlasdb.config.RocksDbPersistentStorageConfig;
import com.palantir.atlasdb.persistent.api.PersistentStore;
import java.io.File;
import java.util.List;
import org.assertj.core.util.Files;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public final class DefaultPersistentStorageFactoryTests {
    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder(Files.currentFolder());

    @Test
    public void createsPersistentStorage() throws Exception {
        RocksDbPersistentStorageConfig config = createRocksDbConfig(testFolder.getRoot());
        PersistentStore persistentStore = new DefaultPersistentStorageFactory().constructPersistentStore(config);

        assertThat(testFolderContent()).hasSize(1);
        assertThat(testFolderContent().get(0).listFiles()).hasSize(1);

        persistentStore.close();

        assertThat(testFolderContent().get(0).listFiles()).isEmpty();
    }

    @Test
    public void createsMultiplePersistentStores() throws Exception {
        PersistentStorageFactory factory = new DefaultPersistentStorageFactory();

        PersistentStore firstStore = factory.constructPersistentStore(createRocksDbConfig(testFolder.newFolder()));
        PersistentStore secondStore = factory.constructPersistentStore(createRocksDbConfig(testFolder.newFolder()));

        assertThat(firstStore).isNotEqualTo(secondStore);

        firstStore.close();
        secondStore.close();
    }

    private static ImmutableRocksDbPersistentStorageConfig createRocksDbConfig(File file) {
        return ImmutableRocksDbPersistentStorageConfig.builder()
                .storagePath(relativePath(file))
                .build();
    }

    private static String relativePath(File file) {
        return Files.currentFolder().toPath().relativize(file.toPath()).toString();
    }

    private List<File> testFolderContent() {
        return ImmutableList.copyOf(
                MoreObjects.firstNonNull(testFolder.getRoot().listFiles(), new File[0]));
    }
}
