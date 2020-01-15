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

package com.palantir.atlasdb.persistent.api;

import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.palantir.atlasdb.persistent.api.PersistentStore.StoreNamespace;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;

public interface LogicalPersistentStore<K, V> {
    @Nullable
    V get(StoreNamespace storeNamespace, @Nonnull K key) throws
            SafeIllegalArgumentException;

    Map<K, V> multiGet(StoreNamespace storeNamespace, List<K> keys);

    void put(StoreNamespace storeNamespace, @Nonnull K key, @Nonnull V value)
            throws SafeIllegalArgumentException;

    void multiPut(StoreNamespace storeNamespace, Map<K, V> toWrite);

    StoreNamespace createNamespace(@Nonnull String name);

    void dropNamespace(StoreNamespace storeNamespace) throws SafeIllegalArgumentException;
}
