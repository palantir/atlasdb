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

import com.palantir.atlasdb.config.ImmutableRocksDbPersistentStorageConfig;
import com.palantir.atlasdb.config.RocksDbPersistentStorageConfig;
import com.palantir.atlasdb.persistent.api.PersistentTimestampStore;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;

public final class DefaultPersistentStorageFactoryTests {
    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    @Test
    public void createsPersistentStorage() throws Exception {
        RocksDbPersistentStorageConfig config = ImmutableRocksDbPersistentStorageConfig.builder()
                .storagePath(testFolder.getRoot().getAbsolutePath())
                .build();
        PersistentTimestampStore persistentTimestampStore = new DefaultPersistentStorageFactory()
                .constructPersistentTimestampStore(config);

        assertThat(testFolder.getRoot().listFiles()).hasSize(1);

        persistentTimestampStore.close();

        assertThat(testFolder.getRoot().listFiles()).isEmpty();
    }

    @Test
    public void createsMultiplePersistentStores() throws Exception {
        RocksDbPersistentStorageConfig config = ImmutableRocksDbPersistentStorageConfig.builder()
                .storagePath(testFolder.getRoot().getAbsolutePath())
                .build();
        PersistentStorageFactory factory = new DefaultPersistentStorageFactory();

        PersistentTimestampStore firstStore = factory.constructPersistentTimestampStore(config);
        PersistentTimestampStore secondStore = factory.constructPersistentTimestampStore(config);

        assertThat(firstStore).isNotEqualTo(secondStore);

        assertThat(testFolder.getRoot().listFiles()).hasSize(2);

        firstStore.close();
        secondStore.close();

        assertThat(testFolder.getRoot().listFiles()).isEmpty();
    }
}
