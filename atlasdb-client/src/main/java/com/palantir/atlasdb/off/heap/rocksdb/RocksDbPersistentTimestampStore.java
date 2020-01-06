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

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.off.heap.ImmutableStoreNamespace;
import com.palantir.atlasdb.off.heap.PersistentTimestampStore;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.logsafe.Preconditions;
import com.palantir.tracing.Tracers.ThrowingCallable;

public final class RocksDbPersistentTimestampStore implements PersistentTimestampStore {
    private static final Logger log = LoggerFactory.getLogger(RocksDbPersistentTimestampStore.class);

    private final ConcurrentHashMap<UUID, ColumnFamilyHandle> availableColumnFamilies = new ConcurrentHashMap<>();
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
        byte[] value = getWithExceptionHandling(availableColumnFamilies.get(storeNamespace.uniqueName()), byteKeyValue);

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

        putWithExceptionHandling(availableColumnFamilies.get(storeNamespace.uniqueName()), key, value);
    }

    @Override
    public StoreNamespace createNamespace(@Nonnull String name) {
        Preconditions.checkNotNull(name, "Namespace should not have a null name");

        return ImmutableStoreNamespace.builder()
                .humanReadableName(name)
                .uniqueName(createColumnFamily())
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
}
