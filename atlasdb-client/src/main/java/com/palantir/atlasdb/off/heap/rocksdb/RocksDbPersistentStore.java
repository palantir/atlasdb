/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.off.heap.rocksdb;

import java.util.Collection;
import java.util.SortedMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.palantir.atlasdb.local.storage.api.ImmutableStoreNamespace;
import com.palantir.atlasdb.local.storage.api.PersistentStore;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.tracing.Tracers.ThrowingCallable;

public final class RocksDbPersistentStore implements PersistentStore {
    private static final Logger log = LoggerFactory.getLogger(RocksDbPersistentStore.class);

    private final ConcurrentHashMap<UUID, ColumnFamilyHandle> availableColumnFamilies = new ConcurrentHashMap<>();
    private final RocksDB rocksDB;

    public RocksDbPersistentStore(RocksDB rocksDB) {
        this.rocksDB = rocksDB;
    }

    @Override
    public void dropNamespace(StoreNamespace<?, ?> storeNamespace) {
        if (!availableColumnFamilies.containsKey(storeNamespace.uniqueName())) {
            throw new SafeIllegalArgumentException("Store namespace does not exist");
        }

        dropColumnFamily(availableColumnFamilies.get(storeNamespace.uniqueName()));
        availableColumnFamilies.remove(storeNamespace.uniqueName());
    }

    @Override
    public <K extends Comparable<K>> Collection<K> loadNamespaceKeys(StoreNamespace<K, ?> storeNamespace) {
        RocksIterator rocksIterator = rocksDB.newIterator(availableColumnFamilies.get(storeNamespace.uniqueName()));
        ImmutableList.Builder<K> builder = ImmutableList.builder();
        rocksIterator.seekToFirst();
        while (rocksIterator.isValid()) {
            builder.add(storeNamespace.serializer().deserializeKey(rocksIterator.key()));
            rocksIterator.next();
        }
        return builder.build();
    }

    @Override
    public <K extends Comparable<K>, V> SortedMap<K, V> loadNamespaceEntries(StoreNamespace<K, V> storeNamespace) {
        RocksIterator rocksIterator = rocksDB.newIterator(availableColumnFamilies.get(storeNamespace.uniqueName()));
        ImmutableSortedMap.Builder<K, V> builder = ImmutableSortedMap.naturalOrder();
        rocksIterator.seekToFirst();
        Serializer<K, V> serializer = storeNamespace.serializer();
        while (rocksIterator.isValid()) {
            K key = serializer.deserializeKey(rocksIterator.key());
            builder.put(key, serializer.deserializeValue(key, rocksIterator.value()));
            rocksIterator.next();
        }
        return builder.build();
    }

    @Override
    public void close() {
        rocksDB.close();
    }

    private void dropColumnFamily(ColumnFamilyHandle columnFamilyHandle) {
        callWithExceptionHandling(() -> {
            rocksDB.dropColumnFamily(columnFamilyHandle);
            return null;
        });
    }

    private byte[] getWithExceptionHandling(ColumnFamilyHandle columnFamilyHandle, byte[] key) {
        return callWithExceptionHandling(() -> rocksDB.get(columnFamilyHandle, key));
    }

    private void putWithExceptionHandling(ColumnFamilyHandle columnFamilyHandle, byte[] key, byte[] value) {
        callWithExceptionHandling(() -> {
            rocksDB.put(columnFamilyHandle, key, value);
            return null;
        });
    }

    private static <T> T callWithExceptionHandling(ThrowingCallable<T, RocksDBException> throwingCallable) {
        try {
            return throwingCallable.call();
        } catch (RocksDBException exception) {
            log.warn("Rocks db raised an exception", exception);
            throw new RuntimeException(exception);
        }
    }

    @Override
    public <K extends Comparable<K>, V> V get(StoreNamespace<K, V> storeNamespace, K key) {
        if (!availableColumnFamilies.containsKey(storeNamespace.uniqueName())) {
            throw new SafeIllegalArgumentException("Store namespace does not exist");
        }

        Serializer<K, V> serializer = storeNamespace.serializer();
        byte[] byteKeyValue = serializer.serializeKey(key);
        byte[] value = getWithExceptionHandling(availableColumnFamilies.get(storeNamespace.uniqueName()), byteKeyValue);

        if (value == null) {
            return null;
        }
        return serializer.deserializeValue(key, value);
    }

    @Override
    public <K extends Comparable<K>, V> void put(StoreNamespace<K, V> storeNamespace, K key, V value) {
        if (!availableColumnFamilies.containsKey(storeNamespace.uniqueName())) {
            throw new SafeIllegalArgumentException("Store namespace does not exist");
        }

        Serializer<K, V> serializer = storeNamespace.serializer();
        putWithExceptionHandling(
                availableColumnFamilies.get(storeNamespace.uniqueName()),
                serializer.serializeKey(key),
                serializer.serializeValue(key, value));
    }

    @Override
    public <K extends Comparable<K>, V> StoreNamespace<K, V> createNamespace(String name, Serializer<K, V> serializer) {
        UUID randomUuid = UUID.randomUUID();
        ColumnFamilyHandle columnFamilyHandle = callWithExceptionHandling(() ->
                rocksDB.createColumnFamily(new ColumnFamilyDescriptor(randomUuid.toString().getBytes())));
        availableColumnFamilies.put(randomUuid, columnFamilyHandle);

        return ImmutableStoreNamespace.<K, V>builder()
                .serializer(serializer)
                .humanReadableName(name)
                .uniqueName(randomUuid)
                .build();
    }
}
