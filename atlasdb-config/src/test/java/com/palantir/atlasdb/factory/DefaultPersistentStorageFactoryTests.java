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

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ProvideSystemProperty;
import org.junit.rules.TemporaryFolder;

import com.palantir.atlasdb.config.ImmutableRocksDbPersistentStorageConfig;
import com.palantir.atlasdb.config.RocksDbPersistentStorageConfig;
import com.palantir.atlasdb.persistent.api.PersistentTimestampStore;

public final class DefaultPersistentStorageFactoryTests {
    @ClassRule
    public static final TemporaryFolder TEST_FOLDER = new TemporaryFolder();

    @Rule
    public ProvideSystemProperty properties
            = new ProvideSystemProperty("user.dir", TEST_FOLDER.getRoot().getAbsolutePath());

    @Test
    public void createsPersistentStorage() throws Exception {
        File folder = TEST_FOLDER.newFolder();
        RocksDbPersistentStorageConfig config = ImmutableRocksDbPersistentStorageConfig.builder()
                .storagePath(TEST_FOLDER.getRoot().toPath().relativize(folder.toPath()).toString())
                .build();
        PersistentTimestampStore persistentTimestampStore = new DefaultPersistentStorageFactory()
                .constructPersistentTimestampStore(config);

        assertThat(folder.listFiles()).hasSize(1);

        persistentTimestampStore.close();

        assertThat(folder.listFiles()).isEmpty();
    }

    @Test
    public void createsMultiplePersistentStores() throws Exception {
        File folder = TEST_FOLDER.newFolder();
        RocksDbPersistentStorageConfig config = ImmutableRocksDbPersistentStorageConfig.builder()
                .storagePath(TEST_FOLDER.getRoot().toPath().relativize(folder.toPath()).toString())
                .build();
        PersistentStorageFactory factory = new DefaultPersistentStorageFactory();

        PersistentTimestampStore firstStore = factory.constructPersistentTimestampStore(config);
        PersistentTimestampStore secondStore = factory.constructPersistentTimestampStore(config);

        assertThat(firstStore).isNotEqualTo(secondStore);

        assertThat(folder.listFiles()).hasSize(2);

        firstStore.close();
        secondStore.close();

        assertThat(folder.listFiles()).isEmpty();
    }
}
