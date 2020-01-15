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

package com.palantir.atlasdb.cache;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.persistent.api.PersistentStore.StoreNamespace;
import com.palantir.atlasdb.persistent.rocksdb.RocksDbPersistentStore;

public final class TimestampStoreTest {
    @ClassRule
    public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();
    private static final String DEFAULT_NAMESPACE_NAME = "default";

    private TimestampStore timestampStore;
    private RocksDbPersistentStore persistentStore;
    private StoreNamespace defaultNamespace;

    @Before
    public void before() throws RocksDBException, IOException {
        File databaseFolder = TEMPORARY_FOLDER.newFolder();
        RocksDB rocksDb = RocksDB.open(databaseFolder.getAbsolutePath());

        persistentStore = new RocksDbPersistentStore(rocksDb, databaseFolder);
        timestampStore = new TimestampStore(persistentStore);

        defaultNamespace = persistentStore.createNamespace(DEFAULT_NAMESPACE_NAME);
    }

    @After
    public void tearDown() throws IOException {
        persistentStore.close();
    }

    @Test
    public void emptyResult() {
        assertThat(timestampStore.get(defaultNamespace, 1L)).isNull();
    }

    @Test
    public void valueIsCorrectlyStored() {
        timestampStore.put(defaultNamespace, 1L, 2L);

        assertThat(timestampStore.get(defaultNamespace, 1L)).isEqualTo(2L);
    }

    @Test
    public void multiGetFilters() {
        timestampStore.put(defaultNamespace, 1L, 2L);

        assertThat(timestampStore.multiGet(defaultNamespace, ImmutableList.of(1L, 3L)))
                .containsExactly(Maps.immutableEntry(1L, 2L));
    }

    @Test
    public void multiPutCorrectlyStores() {
        timestampStore.multiPut(defaultNamespace, ImmutableMap.of(1L, 2L, 3L, 4L));

        assertThat(timestampStore.get(defaultNamespace, 1L)).isEqualTo(2L);
        assertThat(timestampStore.get(defaultNamespace, 3L)).isEqualTo(4L);
        assertThat(timestampStore.get(defaultNamespace, 5L)).isNull();
    }
}
