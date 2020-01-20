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
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.persistent.api.PersistentStore;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.common.streams.KeyedStream;

import okio.ByteString;

/**
 * Stores timestamps using delta encoding for commit timestamp.
 */
public class TimestampStore implements PersistentStore<Long, Long> {
    private final PersistentStore<ByteString, ByteString> persistentStore;

    public TimestampStore(PersistentStore<ByteString, ByteString> persistentStore) {
        this.persistentStore = persistentStore;
    }

    @Nullable
    @Override
    public Optional<Long> get(EntryFamilyHandle entryFamilyHandle, @Nonnull Long startTs) {
        ByteString byteKeyValue = toByteString(startTs);
        return persistentStore.get(entryFamilyHandle, byteKeyValue)
                .map(value -> deserializeValue(startTs, value));
    }

    @Override
    public Map<Long, Long> get(EntryFamilyHandle entryFamilyHandle, List<Long> keys) {

        List<ByteString> byteKeys = keys.stream()
                .map(ValueType.VAR_LONG::convertFromJava)
                .map(ByteString::of)
                .collect(Collectors.toList());

        Map<ByteString, ByteString> byteValues = persistentStore.get(entryFamilyHandle, byteKeys);

        if (byteValues.isEmpty()) {
            return ImmutableMap.of();
        }

        return KeyedStream.stream(byteValues)
                .mapEntries(TimestampStore::deserializeEntry)
                .collectToMap();
    }

    @Override
    public void put(EntryFamilyHandle entryFamilyHandle, @Nonnull Long startTs, @Nonnull Long commitTs) {
        ByteString key = toByteString(startTs);
        ByteString value = toByteString(commitTs - startTs);

        persistentStore.put(entryFamilyHandle, key, value);
    }

    @Override
    public void put(EntryFamilyHandle entryFamilyHandle, Map<Long, Long> toWrite) {
        KeyedStream.stream(toWrite).forEach((key, value) -> put(entryFamilyHandle, key, value));
    }

    @Override
    public EntryFamilyHandle createEntryFamily() {
        return persistentStore.createEntryFamily();
    }

    @Override
    public void dropEntryFamily(EntryFamilyHandle entryFamilyHandle) {
        persistentStore.dropEntryFamily(entryFamilyHandle);
    }

    @Override
    public void close() throws Exception {
        persistentStore.close();
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
}
