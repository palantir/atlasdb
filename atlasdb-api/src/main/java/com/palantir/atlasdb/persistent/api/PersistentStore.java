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
import java.util.UUID;

import javax.annotation.Nonnull;

import org.immutables.value.Value;

import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;

public interface PersistentStore<K, V> extends AutoCloseable {
    @Value.Immutable
    interface StoreHandle {
        UUID uniqueName();
    }

    /**
     * Gets the value associated with the entry specified by {@code key}.
     *
     * @param storeHandle handle to the namespace from which we want to retrieve the value
     * @param key            entry key for which we want to retrieve the value
     * @return the {@link Optional} containing the value or empty if there is no associated value
     * @throws SafeIllegalArgumentException when {@code storeHandle} is a handle to a non existing namespace
     */
    Optional<V> get(StoreHandle storeHandle, @Nonnull K key);

    /**
     * Gets the values associated with the entries specified by {@code keys}.
     *
     * @param storeHandle handle to the namespace from which we want to retrieve the values
     * @param keys           representing keys for which we want to retrieve the values
     * @return a map from keys to values
     */
    Map<K, V> get(StoreHandle storeHandle, List<K> keys);

    /**
     * Stores the {@code value} for the associated {@code key} while overwriting the existing value in the specified
     * {@code storeHandle}.
     *
     * @param storeHandle of the store to which we should store the entry
     * @param key            entry key
     * @param value          entry value
     * @throws SafeIllegalArgumentException when {@code storeHandle} is a handle to a non existing namespace
     */
    void put(StoreHandle storeHandle, @Nonnull K key, @Nonnull V value);

    /**
     * Stores the entry pairs given in {@code toWrite}, overwriting the existing values.
     *
     * @param storeHandle of the store to which we should store the entry
     * @param toWrite        entry pairs to write
     * @throws SafeIllegalArgumentException when {@code storeHandle} is a handle to a non existing namespace
     */
    void put(StoreHandle storeHandle, Map<K, V> toWrite);

    /**
     * Creates a handle of type {@link StoreHandle} to the underlying store. Each call returns a new {@link StoreHandle}
     *
     * @return {@link StoreHandle} which represents a handle to the created namespace
     */
    StoreHandle createStoreHandle();

    /**
     * Drops the namespace specified by the supplied handle. Dropping of a namespace may fail if there are concurrent
     * calls on the same namespace or if the namespace has already been dropped.
     *
     * @param storeHandle handle
     * @throws SafeIllegalArgumentException if the {@code storeHandle} does not exist
     */
    void dropStoreHandle(StoreHandle storeHandle);
}
