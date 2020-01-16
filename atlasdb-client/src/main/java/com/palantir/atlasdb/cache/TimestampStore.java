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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.persistent.api.PersistentStore;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.common.streams.KeyedStream;

import okio.ByteString;

/**
 * Stores timestamps using delta encoding for commit timestamp.
 */
public final class TimestampStore implements PersistentStore<Long, Long> {
    private final PersistentStore<ByteString, ByteString> physicalPersistentStore;

    public TimestampStore(PersistentStore<ByteString, ByteString> persistentStore) {
        this.physicalPersistentStore = persistentStore;
    }

    @Override
    public Optional<Long> get(StoreHandle storeHandle, @Nonnull Long startTs) {
        ByteString byteKeyValue = toByteString(startTs);
        return physicalPersistentStore.get(storeHandle, byteKeyValue)
                .map(value -> deserializeValue(startTs, value));
    }

    @Override
    public Map<Long, Long> get(StoreHandle storeHandle, List<Long> keys) {
        List<ByteString> byteKeys = keys.stream()
                .map(ValueType.VAR_LONG::convertFromJava)
                .map(ByteString::of)
                .collect(Collectors.toList());

        Map<ByteString, ByteString> byteValues = physicalPersistentStore.get(storeHandle, byteKeys);

        if (byteValues.isEmpty()) {
            return ImmutableMap.of();
        }

        return KeyedStream.stream(byteValues)
                .mapEntries(TimestampStore::deserializeEntry)
                .collectToMap();
    }

    @Override
    public void put(StoreHandle storeHandle, @Nonnull Long startTs, @Nonnull Long commitTs) {
        ByteString key = toByteString(startTs);
        ByteString value = toByteString(commitTs - startTs);

        physicalPersistentStore.put(storeHandle, key, value);
    }

    @Override
    public void put(StoreHandle storeHandle, Map<Long, Long> toWrite) {
        KeyedStream.stream(toWrite).forEach((key, value) -> put(storeHandle, key, value));
    }

    @Override
    public StoreHandle createStoreHandle() {
        return physicalPersistentStore.createStoreHandle();
    }

    @Override
    public void dropStoreHandle(StoreHandle storeHandle) {
        physicalPersistentStore.dropStoreHandle(storeHandle);
    }

    private static Map.Entry<Long, Long> deserializeEntry(ByteString key, ByteString value) {
        Long deserializedKey = (Long) ValueType.VAR_LONG.convertToJava(key.toByteArray(), 0);
        return Maps.immutableEntry(deserializedKey, deserializeValue(deserializedKey, value));
    }

    private static Long deserializeValue(Long key, ByteString value) {
        if (value == null) {
            return null;
        }
        return key + (Long) ValueType.VAR_LONG.convertToJava(value.toByteArray(), 0);
    }

    private static ByteString toByteString(@Nonnull Long startTs) {
        return ByteString.of(ValueType.VAR_LONG.convertFromJava(startTs));
    }

    @Override
    public void close() throws Exception {
        physicalPersistentStore.close();
    }
}
