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

import com.palantir.atlasdb.persistent.api.PersistentStore;
import com.palantir.atlasdb.persistent.rocksdb.RocksDbPersistentStore;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.test.utils.SubdirectoryCreator;
import java.io.File;
import java.io.IOException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public final class RocksDbOffHeapTimestampCacheIntegrationTests {
    @TempDir
    public static File TEMPORARY_FOLDER;

    private static final int CACHE_SIZE = 2;

    private TimestampCache offHeapTimestampCache;
    private PersistentStore persistentStore;

    @BeforeEach
    public void before() throws RocksDBException, IOException {
        File databaseFolder = SubdirectoryCreator.createAndGetSubdirectory(TEMPORARY_FOLDER, "temp.txt");
        RocksDB rocksDb = RocksDB.open(databaseFolder.getAbsolutePath());

        persistentStore = new RocksDbPersistentStore(rocksDb, databaseFolder);

        offHeapTimestampCache = OffHeapTimestampCache.create(
                persistentStore, MetricsManagers.createForTests().getTaggedRegistry(), () -> CACHE_SIZE);
    }

    @AfterEach
    public void after() throws Exception {
        persistentStore.close();
    }

    @Test
    public void cachedEntry() {
        offHeapTimestampCache.putAlreadyCommittedTransaction(1L, 3L);

        assertThat(offHeapTimestampCache.getCommitTimestampIfPresent(1L)).isEqualTo(3L);
    }

    @Test
    public void nonCachedEntry() {
        assertThat(offHeapTimestampCache.getCommitTimestampIfPresent(1L)).isNull();
    }

    @Test
    public void cacheNukedWhenSizeLimitExceeded() {
        offHeapTimestampCache.putAlreadyCommittedTransaction(1L, 3L);
        offHeapTimestampCache.putAlreadyCommittedTransaction(2L, 4L);
        offHeapTimestampCache.putAlreadyCommittedTransaction(5L, 6L);

        assertThat(offHeapTimestampCache.getCommitTimestampIfPresent(1L)).isNull();
        assertThat(offHeapTimestampCache.getCommitTimestampIfPresent(2L)).isNull();
        assertThat(offHeapTimestampCache.getCommitTimestampIfPresent(5L)).isEqualTo(6L);
    }

    @Test
    public void clearCache() {
        offHeapTimestampCache.putAlreadyCommittedTransaction(1L, 3L);
        assertThat(offHeapTimestampCache.getCommitTimestampIfPresent(1L)).isEqualTo(3L);

        offHeapTimestampCache.clear();
        assertThat(offHeapTimestampCache.getCommitTimestampIfPresent(1L)).isNull();
    }
}
