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
    /**
     * Represents a handle to the underlying space of key-value pairs. Handle is linked with one underlying store space.
     */
    @Value.Immutable
    interface Handle {
        UUID uniqueName();
    }

    /**
     * Gets the value associated with the entry specified by {@code key}.
     *
     * @param handle handle to the store space from which we want to retrieve the value
     * @param key    entry key for which we want to retrieve the value
     * @return the {@link Optional} containing the value or empty if there is no associated value
     * @throws SafeIllegalArgumentException when referencing a non existing store space
     */
    Optional<V> get(PersistentStore.Handle handle, @Nonnull K key);

    /**
     * Gets the values associated with the entries specified by {@code keys}.
     *
     * @param handle handle to the store space
     * @param keys   representing keys for which we want to retrieve the values
     * @return a map from keys to values
     */
    Map<K, V> get(PersistentStore.Handle handle, List<K> keys);

    /**
     * Stores the {@code value} for the associated {@code key} while overwriting the existing value in the specified
     * store space.
     *
     * @param handle of the store to which we should store the entry
     * @param key    entry key
     * @param value  entry value
     * @throws SafeIllegalArgumentException when referencing a non existing store space
     */
    void put(PersistentStore.Handle handle, @Nonnull K key, @Nonnull V value);

    /**
     * Stores the entry pairs given in {@code toWrite}, overwriting the existing values.
     *
     * @param handle  of the store space to which we should store the entry
     * @param toWrite entry pairs to write
     * @throws SafeIllegalArgumentException when referencing a non existing store space
     */
    void put(PersistentStore.Handle handle, Map<K, V> toWrite);

    /**
     * Creates a store space to be used to store key-value pairs. Each call creates a new store space.
     *
     * @return handle to the created store space.
     */
    PersistentStore.Handle createSpace();

    /**
     * Drops the store spacey specified by the supplied handle. Dropping of a store space may fail if there are
     * concurrent calls on the same store space or if the store space has already been dropped.
     *
     * @param handle handle
     * @throws SafeIllegalArgumentException if the {@code handle} points to a non-existing store space
     */
    void dropStoreSpace(PersistentStore.Handle handle);
}
