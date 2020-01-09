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

import java.io.File;
import java.util.UUID;
import java.util.regex.Pattern;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.MoreObjects;
import com.palantir.atlasdb.config.RocksDbPersistentStorageConfig;
import com.palantir.atlasdb.persistent.api.PersistentTimestampStore;
import com.palantir.atlasdb.persistent.rocksdb.RocksDbPersistentTimestampStore;
import com.palantir.logsafe.Preconditions;

public final class PersistentStorageFactory {
    private static final Logger log = LoggerFactory.getLogger(PersistentStorageFactory.class);
    private static final Pattern UUID_PATTERN = Pattern.compile(
            "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");

    /**
     * For the given path does the following: 1) if it exists checks that it is a directory, 2) if it is a directory
     * removes all subfolders whose names are string representation of a UUID.
     *
     * @param storagePath to the proposed storage location
     */
    public static void sanitizeStoragePath(String storagePath) {
        File storageDirectory = new File(storagePath);
        if (!storageDirectory.exists()) {
            Preconditions.checkState(storageDirectory.mkdir(), "Not able to create a storage directory");
            return;
        }
        Preconditions.checkArgument(storageDirectory.isDirectory(), "Storage path has to be a directory");
        for (File file : MoreObjects.firstNonNull(storageDirectory.listFiles(), new File[0])) {
            if (file.isDirectory()) {
                if (UUID_PATTERN.matcher(file.getName()).matches()) {
                    file.delete();
                }
            }
        }
    }

    /**
     * Constructs a {@link PersistentTimestampStore} from a {@link RocksDbPersistentStorageConfig}.
     *
     * @param config of the requested RocksDB persistent storage
     * @return RockDB implementation of {@link PersistentTimestampStore}
     */
    public PersistentTimestampStore constructPersistentTimestampStore(RocksDbPersistentStorageConfig config) {
        File databaseFolder = new File(config.storagePath(), UUID.randomUUID().toString());
        Preconditions.checkState(databaseFolder.mkdir(), "RocksDb directory created");
        RocksDB rocksDb = openRocksConnection(databaseFolder);
        return new RocksDbPersistentTimestampStore(rocksDb, databaseFolder);
    }

    private static RocksDB openRocksConnection(File databaseFolder) {
        try {
            return RocksDB.open(databaseFolder.getAbsolutePath());
        } catch (RocksDBException e) {
            log.error("Opening RocksDB failed", e);
            throw new RuntimeException(e);
        }
    }
}
