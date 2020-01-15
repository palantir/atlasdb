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

package com.palantir.atlasdb.persistent.rocksdb;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import com.palantir.atlasdb.persistent.api.ImmutableStoreNamespace;
import com.palantir.atlasdb.persistent.api.PersistentStore;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.Preconditions;
import com.palantir.tracing.Tracers.ThrowingCallable;

/**
 * Implementation of the {@link PersistentStore} using RocksDB as the underlying persistent storage. Commit
 * timestamp associated with the start timestamp is encoded using delta encoding. Created {@link StoreNamespace}s are
 * backed by RocksDB ColumnFamilies such that calling {@link RocksDbPersistentStore#createNamespace(String)}
 * with the same name will construct a new {@link ColumnFamilyHandle} for each call.
 */
public final class RocksDbPersistentStore implements PersistentStore {
    private static final Logger log = LoggerFactory.getLogger(RocksDbPersistentStore.class);

    private final ConcurrentMap<UUID, ColumnFamilyHandle> availableColumnFamilies = new ConcurrentHashMap<>();
    private final RocksDB rocksDB;
    private final File databaseFolder;

    public RocksDbPersistentStore(RocksDB rocksDB, File databaseFolder) {
        this.rocksDB = rocksDB;
        this.databaseFolder = databaseFolder;
    }

    @Override
    @Nullable
    public byte[] get(StoreNamespace storeNamespace, @Nonnull byte[] key) {
        checkNamespaceExists(storeNamespace);

        return getValueBytes(availableColumnFamilies.get(storeNamespace.uniqueName()), key);
    }

    @Override
    public Map<byte[], byte[]> multiGet(StoreNamespace storeNamespace, List<byte[]> keys) {
        checkNamespaceExists(storeNamespace);

        List<byte[]> byteValues = multiGetValueBytes(
                availableColumnFamilies.get(storeNamespace.uniqueName()),
                keys);

        if (byteValues.isEmpty()) {
            return ImmutableMap.of();
        }

        return KeyedStream.ofEntries(
                Streams.zip(
                        keys.stream(),
                        byteValues.stream(),
                        Maps::immutableEntry))
                .filter(Objects::nonNull)
                .collectToMap();
    }

    @Override
    public void put(StoreNamespace storeNamespace, @Nonnull byte[] key, @Nonnull byte[] value) {
        checkNamespaceExists(storeNamespace);
        putEntry(availableColumnFamilies.get(storeNamespace.uniqueName()), key, value);
    }

    @Override
    public void multiPut(StoreNamespace storeNamespace, Map<byte[], byte[]> toWrite) {
        KeyedStream.stream(toWrite).forEach((key, value) -> put(storeNamespace, key, value));
    }

    @Override
    public StoreNamespace createNamespace(@Nonnull String name) {
        UUID columnFamily = createColumnFamily();
        return ImmutableStoreNamespace.builder()
                .humanReadableName(name)
                .uniqueName(columnFamily)
                .build();
    }

    @Override
    public void dropNamespace(StoreNamespace storeNamespace) {
        checkNamespaceExists(storeNamespace);

        dropColumnFamily(storeNamespace);
    }

    private void checkNamespaceExists(StoreNamespace storeNamespace) {
        Preconditions.checkArgument(
                availableColumnFamilies.containsKey(storeNamespace.uniqueName()),
                "Store namespace does not exist");
    }

    @Override
    public void close() throws IOException {
        rocksDB.close();

        // by sorting the walked paths in the reverse lexicographical order we will first delete all sub-folders/files
        // before the folder itself basically doing a rm -rf .
        Path absoluteStoragePath = new File(databaseFolder.getAbsolutePath()).toPath();
        try (Stream<Path> stream = Files.walk(absoluteStoragePath)) {
            List<Path> sortedPaths = stream.sorted(Comparator.reverseOrder()).collect(Collectors.toList());
            for (Path filePath : sortedPaths) {
                Files.delete(filePath);
            }
        }
    }

    private UUID createColumnFamily() {
        UUID randomUuid = UUID.randomUUID();
        ColumnFamilyHandle columnFamilyHandle = callWithExceptionHandling(() ->
                rocksDB.createColumnFamily(new ColumnFamilyDescriptor(randomUuid.toString().getBytes())));
        availableColumnFamilies.put(randomUuid, columnFamilyHandle);
        return randomUuid;
    }

    private void dropColumnFamily(StoreNamespace storeNamespace) {
        callWithExceptionHandling(() -> {
            rocksDB.dropColumnFamily(availableColumnFamilies.get(storeNamespace.uniqueName()));
            return null;
        });
        availableColumnFamilies.remove(storeNamespace.uniqueName());
    }

    private byte[] getValueBytes(ColumnFamilyHandle columnFamilyHandle, byte[] key) {
        try {
            return rocksDB.get(columnFamilyHandle, key);
        } catch (RocksDBException exception) {
            log.warn("Rocks db raised an exception", exception);
            return null;
        }
    }

    private List<byte[]> multiGetValueBytes(ColumnFamilyHandle columnFamilyHandle, List<byte[]> keys) {
        try {
            return rocksDB.multiGetAsList(Collections.nCopies(keys.size(), columnFamilyHandle), keys);
        } catch (RocksDBException exception) {
            log.warn("Rocks db raised an exception", exception);
            return ImmutableList.of();
        }
    }

    private void putEntry(ColumnFamilyHandle columnFamilyHandle, byte[] key, byte[] value) {
        try {
            rocksDB.put(columnFamilyHandle, key, value);
        } catch (RocksDBException exception) {
            log.warn("Rocks db raised an exception", exception);
        }
    }

    private static <T> T callWithExceptionHandling(ThrowingCallable<T, RocksDBException> throwingCallable) {
        try {
            return throwingCallable.call();
        } catch (RocksDBException exception) {
            log.warn("Rocks db raised an exception", exception);
            throw new RuntimeException(exception);
        }
    }
}
