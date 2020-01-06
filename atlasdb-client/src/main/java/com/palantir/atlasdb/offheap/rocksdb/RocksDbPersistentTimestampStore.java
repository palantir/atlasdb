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

package com.palantir.atlasdb.offheap.rocksdb;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.offheap.ImmutableStoreNamespace;
import com.palantir.atlasdb.offheap.PersistentTimestampStore;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.logsafe.Preconditions;
import com.palantir.tracing.Tracers.ThrowingCallable;

/**
 * Implementation of the {@link PersistentTimestampStore} using RocksDB as the underlying persistent storage. Commit
 * timestamp associated with the start timestamp is encoded using delta encoding. Created {@link StoreNamespace}s are
 * backed by RocksDB ColumnFamilies such that calling {@link RocksDbPersistentTimestampStore#createNamespace(String)}
 * with the same name will construct a new {@link ColumnFamilyHandle} for each call.
 */
public final class RocksDbPersistentTimestampStore implements PersistentTimestampStore {
    private static final Logger log = LoggerFactory.getLogger(RocksDbPersistentTimestampStore.class);

    private final ConcurrentMap<UUID, ColumnFamilyHandle> availableColumnFamilies = new ConcurrentHashMap<>();
    private final RocksDB rocksDB;

    public RocksDbPersistentTimestampStore(RocksDB rocksDB) {
        this.rocksDB = rocksDB;
    }

    @Override
    @Nullable
    public Long get(StoreNamespace storeNamespace, @Nonnull Long startTs) {
        Preconditions.checkArgument(
                availableColumnFamilies.containsKey(storeNamespace.uniqueName()),
                "Store namespace does not exist");

        byte[] byteKeyValue = ValueType.VAR_LONG.convertFromJava(startTs);
        byte[] value = getValueBytes(availableColumnFamilies.get(storeNamespace.uniqueName()), byteKeyValue);

        if (value == null) {
            return null;
        }
        return startTs + (Long) ValueType.VAR_LONG.convertToJava(value, 0);
    }

    @Override
    public void put(StoreNamespace storeNamespace, @Nonnull Long startTs, @Nonnull Long commitTs) {
        Preconditions.checkArgument(
                availableColumnFamilies.containsKey(storeNamespace.uniqueName()),
                "Store namespace does not exist");

        byte[] key = ValueType.VAR_LONG.convertFromJava(startTs);
        byte[] value = ValueType.VAR_LONG.convertFromJava(commitTs - startTs);

        putEntry(availableColumnFamilies.get(storeNamespace.uniqueName()), key, value);
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
        Preconditions.checkArgument(
                availableColumnFamilies.containsKey(storeNamespace.uniqueName()),
                "Store namespace does not exist");

        dropColumnFamily(storeNamespace);
    }

    @Override
    public void close() {
        rocksDB.close();
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
