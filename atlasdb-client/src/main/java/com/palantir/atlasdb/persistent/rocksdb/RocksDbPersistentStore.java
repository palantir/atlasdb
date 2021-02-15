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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import com.palantir.atlasdb.persistent.api.PersistentStore;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.Preconditions;
import com.palantir.tracing.Tracers.ThrowingCallable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import okio.ByteString;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    public Optional<ByteString> get(PersistentStore.Handle handle, @Nonnull ByteString key) {
        checkStoreSpaceExists(handle);

        return getValueBytes(availableColumnFamilies.get(handle.id()), key);
    }

    @Override
    public Map<ByteString, ByteString> get(PersistentStore.Handle handle, List<ByteString> keys) {
        checkStoreSpaceExists(handle);

        List<ByteString> byteValues = multiGetValueByteStrings(availableColumnFamilies.get(handle.id()), keys);

        if (byteValues.isEmpty()) {
            return ImmutableMap.of();
        }

        return KeyedStream.ofEntries(Streams.zip(keys.stream(), byteValues.stream(), Maps::immutableEntry))
                .filter(Objects::nonNull)
                .collectToMap();
    }

    @Override
    public void put(PersistentStore.Handle handle, @Nonnull ByteString key, @Nonnull ByteString value) {
        checkStoreSpaceExists(handle);
        putEntry(availableColumnFamilies.get(handle.id()), key, value);
    }

    @Override
    public void put(PersistentStore.Handle handle, Map<ByteString, ByteString> toWrite) {
        KeyedStream.stream(toWrite).forEach((key, value) -> put(handle, key, value));
    }

    @Override
    public PersistentStore.Handle createSpace() {
        Handle handle = PersistentStore.Handle.newHandle();
        ColumnFamilyHandle columnFamilyHandle = callWithExceptionHandling(() -> rocksDB.createColumnFamily(
                new ColumnFamilyDescriptor(handle.id().toString().getBytes())));
        availableColumnFamilies.put(handle.id(), columnFamilyHandle);
        return handle;
    }

    @Override
    public void dropStoreSpace(PersistentStore.Handle handle) {
        checkStoreSpaceExists(handle);

        dropColumnFamily(handle);
    }

    private void checkStoreSpaceExists(PersistentStore.Handle handle) {
        Preconditions.checkArgument(availableColumnFamilies.containsKey(handle.id()), "Store space does not exist.");
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

    private void dropColumnFamily(PersistentStore.Handle handle) {
        callWithExceptionHandling(() -> {
            rocksDB.dropColumnFamily(availableColumnFamilies.get(handle.id()));
            return null;
        });
        availableColumnFamilies.remove(handle.id());
    }

    private Optional<ByteString> getValueBytes(ColumnFamilyHandle columnFamilyHandle, ByteString key) {
        try {
            return Optional.ofNullable(rocksDB.get(columnFamilyHandle, key.toByteArray()))
                    .map(ByteString::of);
        } catch (RocksDBException exception) {
            log.warn("Rocks db raised an exception", exception);
            return Optional.empty();
        }
    }

    private List<ByteString> multiGetValueByteStrings(ColumnFamilyHandle columnFamilyHandle, List<ByteString> keys) {
        List<byte[]> values = multiGetValueBytes(
                columnFamilyHandle, keys.stream().map(ByteString::toByteArray).collect(Collectors.toList()));
        return values.stream().filter(Objects::nonNull).map(ByteString::of).collect(Collectors.toList());
    }

    private List<byte[]> multiGetValueBytes(ColumnFamilyHandle columnFamilyHandle, List<byte[]> keys) {
        try {
            return rocksDB.multiGetAsList(Collections.nCopies(keys.size(), columnFamilyHandle), keys);
        } catch (RocksDBException exception) {
            log.warn("Rocks db raised an exception", exception);
            return ImmutableList.of();
        }
    }

    private void putEntry(ColumnFamilyHandle columnFamilyHandle, ByteString key, ByteString value) {
        try {
            rocksDB.put(columnFamilyHandle, key.toByteArray(), value.toByteArray());
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
