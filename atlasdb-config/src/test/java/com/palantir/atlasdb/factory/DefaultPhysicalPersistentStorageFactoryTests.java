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

import java.io.File;
import java.util.List;

import org.assertj.core.util.Files;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.config.ImmutableRocksDbPersistentStorageConfig;
import com.palantir.atlasdb.config.RocksDbPersistentStorageConfig;
import com.palantir.atlasdb.persistent.api.PersistentStore;

import okio.ByteString;

public final class DefaultPhysicalPersistentStorageFactoryTests {
    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder(Files.currentFolder());

    private String storagePath;

    @Before
    public void setUp() {
        storagePath = Files.currentFolder()
                .toPath()
                .relativize(testFolder.getRoot().toPath())
                .toString();
    }

    @Test
    public void createsPersistentStorage() throws Exception {
        RocksDbPersistentStorageConfig config = ImmutableRocksDbPersistentStorageConfig.builder()
                .storagePath(storagePath)
                .build();
        PersistentStore<ByteString, ByteString> persistentStore = new DefaultPhysicalPersistentStorageFactory()
                .constructPersistentStore(config);

        assertThat(testFolderContent()).hasSize(1);

        persistentStore.close();

        assertThat(testFolderContent()).isEmpty();
    }

    @Test
    public void createsMultiplePersistentStores() throws Exception {
        RocksDbPersistentStorageConfig config = ImmutableRocksDbPersistentStorageConfig.builder()
                .storagePath(storagePath)
                .build();
        PhysicalPersistentStorageFactory factory = new DefaultPhysicalPersistentStorageFactory();

        PersistentStore<ByteString, ByteString> firstStore = factory.constructPersistentStore(config);
        PersistentStore<ByteString, ByteString> secondStore = factory.constructPersistentStore(config);

        assertThat(firstStore).isNotEqualTo(secondStore);

        assertThat(testFolderContent()).hasSize(2);

        firstStore.close();
        secondStore.close();

        assertThat(testFolderContent()).isEmpty();
    }

    private List<File> testFolderContent() {
        return ImmutableList.copyOf(MoreObjects.firstNonNull(
                testFolder.getRoot().listFiles(),
                new File[0]));
    }
}
