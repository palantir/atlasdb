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
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.persistent.api.PersistentStore;
import com.palantir.atlasdb.persistent.api.PersistentStore.StoreNamespace;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.common.streams.KeyedStream;

class TimestampStore {
    private final PersistentStore persistentStore;

    public TimestampStore(PersistentStore persistentStore) {
        this.persistentStore = persistentStore;
    }

    @Nullable
    public Long get(StoreNamespace storeNamespace, @Nonnull Long startTs) {
        byte[] byteKeyValue = ValueType.VAR_LONG.convertFromJava(startTs);
        byte[] value = persistentStore.get(storeNamespace, byteKeyValue);

        return deserializeValue(startTs, value);
    }

    public Map<Long, Long> multiGet(StoreNamespace storeNamespace, List<Long> keys) {

        List<byte[]> byteKeys = keys.stream()
                .map(ValueType.VAR_LONG::convertFromJava)
                .collect(Collectors.toList());

        Map<byte[], byte[]> byteValues = persistentStore.multiGet(storeNamespace, byteKeys);

        if (byteValues.isEmpty()) {
            return ImmutableMap.of();
        }

        return KeyedStream.stream(byteValues)
                .mapEntries(TimestampStore::deserializeEntry)
                .collectToMap();
    }

    public void put(StoreNamespace storeNamespace, @Nonnull Long startTs, @Nonnull Long commitTs) {
        byte[] key = ValueType.VAR_LONG.convertFromJava(startTs);
        byte[] value = ValueType.VAR_LONG.convertFromJava(commitTs - startTs);

        persistentStore.put(storeNamespace, key, value);
    }

    public void multiPut(StoreNamespace storeNamespace, Map<Long, Long> toWrite) {
        KeyedStream.stream(toWrite).forEach((key, value) -> put(storeNamespace, key, value));
    }

    public StoreNamespace createNamespace(@Nonnull String name) {
        return persistentStore.createNamespace(name);
    }


    public void dropNamespace(StoreNamespace storeNamespace) {
        persistentStore.dropNamespace(storeNamespace);
    }

    private static Map.Entry<Long, Long> deserializeEntry(byte[] key, byte[] value) {
        Long deserializedKey = (Long) ValueType.VAR_LONG.convertToJava(key, 0);
        return Maps.immutableEntry(deserializedKey, deserializeValue(deserializedKey, value));
    }

    private static Long deserializeValue(Long key, byte[] value) {
        if (value == null) {
            return null;
        }
        return key + (Long) ValueType.VAR_LONG.convertToJava(value, 0);
    }
}
