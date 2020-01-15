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

import com.palantir.atlasdb.persistent.api.PersistentStore;
import com.palantir.atlasdb.persistent.rocksdb.RocksDbPersistentStore;
import com.palantir.atlasdb.util.MetricsManagers;

public final class DefaultOffHeapCacheIntegrationTests {
    @ClassRule
    public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();
    private static final int CACHE_SIZE = 2;

    private OffHeapCache<Long, Long> offHeapTimestampCache;
    private PersistentStore persistentStore;

    @Before
    public void before() throws RocksDBException, IOException {
        File databaseFolder = TEMPORARY_FOLDER.newFolder();
        RocksDB rocksDb = RocksDB.open(databaseFolder.getAbsolutePath());

        persistentStore = new RocksDbPersistentStore(rocksDb, databaseFolder);

        offHeapTimestampCache = OffHeapCaches.create(
                new TimestampStore(persistentStore),
                MetricsManagers.createForTests().getTaggedRegistry(),
                () -> CACHE_SIZE);
    }

    @After
    public void after() throws Exception {
        persistentStore.close();
    }

    @Test
    public void cachedEntry() {
        offHeapTimestampCache.put(1L, 3L);

        assertThat(offHeapTimestampCache.get(1L)).isEqualTo(3L);
    }

    @Test
    public void nonCachedEntry() {
        assertThat(offHeapTimestampCache.get(1L)).isNull();
    }

    @Test
    public void cacheNukedWhenSizeLimitExceeded() {
        offHeapTimestampCache.put(1L, 3L);
        offHeapTimestampCache.put(2L, 4L);
        offHeapTimestampCache.put(5L, 6L);

        assertThat(offHeapTimestampCache.get(1L)).isNull();
        assertThat(offHeapTimestampCache.get(2L)).isNull();
        assertThat(offHeapTimestampCache.get(5L)).isEqualTo(6L);
    }

    @Test
    public void clearCache() {
        offHeapTimestampCache.put(1L, 3L);
        assertThat(offHeapTimestampCache.get(1L)).isEqualTo(3L);

        offHeapTimestampCache.clear();
        assertThat(offHeapTimestampCache.get(1L)).isNull();
    }
}
