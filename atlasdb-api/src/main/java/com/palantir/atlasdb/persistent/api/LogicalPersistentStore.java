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
import java.util.Optional;

import javax.annotation.Nonnull;

import com.palantir.atlasdb.persistent.api.PhysicalPersistentStore.StoreNamespace;

public interface LogicalPersistentStore<K, V> {
    /**
     * Retrieve the value associated with the given {@code key}.
     *
     * @param storeNamespace from which to retrieve the value
     * @param key            of the cache entry
     * @return value associated or null if the entry is missing
     */
    Optional<V> get(StoreNamespace storeNamespace, @Nonnull K key);

    /**
     * Retrieves values for all supplied {@code keys}.
     *
     * @param storeNamespace from which to retrieve the values
     * @param keys           for which entries we are interested in
     * @return map of key, value pairs containing only entries which are stored
     */
    Map<K, V> get(StoreNamespace storeNamespace, List<K> keys);

    /**
     * Stores the given entry pair.
     *
     * @param storeNamespace where to store the entry
     * @param key            of the entry
     * @param value          of the entry
     */
    void put(StoreNamespace storeNamespace, @Nonnull K key, @Nonnull V value);

    /**
     * Stores all entry pairs specified in {@code toWrite}.
     *
     * @param storeNamespace where to store the entries
     * @param toWrite        entry pairs to store
     */
    void put(StoreNamespace storeNamespace, Map<K, V> toWrite);

    /**
     * Creates a {@link StoreNamespace} with the given name. Multiple calls with the same {@code name} will return
     * different namespaces.
     *
     * @param name of the namespace
     * @return handle to the underlying namespace
     */
    StoreNamespace createNamespace(@Nonnull String name);

    /**
     * Given the namespace handle represented by {@code storeNamespace} drops the internal structures.
     *
     * @param storeNamespace which should be dropped
     */
    void dropNamespace(StoreNamespace storeNamespace);
}
