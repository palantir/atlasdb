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

import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.Nonnull;
import okio.ByteString;
import org.immutables.value.Value;

public interface PersistentStore extends AutoCloseable {
    /**
     * Represents a handle to the underlying space of key-value pairs. A space of key-value pairs is analogous to a
     * PostgreSQL table or RocksdDb column family. Handle is linked with one underlying store space.
     */
    @Value.Immutable
    @Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE)
    interface Handle {
        UUID id();

        static Handle newHandle() {
            return ImmutableHandle.builder().id(UUID.randomUUID()).build();
        }
    }

    /**
     * Gets the value associated with the entry specified by {@code key}.
     *
     * @param handle of the store space from which we want to retrieve the value
     * @param key    entry key for which we want to retrieve the value
     * @return the {@link Optional} containing the value or empty if there is no associated value
     * @throws SafeIllegalArgumentException when referencing a non existing store space
     */
    Optional<ByteString> get(PersistentStore.Handle handle, @Nonnull ByteString key);

    /**
     * Gets the values associated with the entries specified by {@code keys}. Keys which are not present in the store
     * will not be included in the returned map.
     *
     * @param handle of the store space
     * @param keys   representing keys for which we want to retrieve the values
     * @return a map from keys to values
     */
    Map<ByteString, ByteString> get(PersistentStore.Handle handle, List<ByteString> keys);

    /**
     * Stores the {@code value} for the associated {@code key} while overwriting the existing value in the specified
     * store space.
     *
     * @param handle of the store to which we should store the entry
     * @param key    entry key
     * @param value  entry value
     * @throws SafeIllegalArgumentException when referencing a non existing store space
     */
    void put(PersistentStore.Handle handle, @Nonnull ByteString key, @Nonnull ByteString value);

    /**
     * Stores the entry pairs given in {@code toWrite}, overwriting the existing values.
     *
     * @param handle  of the store space to which we should store the entry
     * @param toWrite entry pairs to write
     * @throws SafeIllegalArgumentException when referencing a non existing store space
     */
    void put(PersistentStore.Handle handle, Map<ByteString, ByteString> toWrite);

    /**
     * Creates a store space to be used to store key-value pairs. Each call creates a new store space.
     *
     * @return handle to the created store space
     */
    PersistentStore.Handle createSpace();

    /**
     * Drops the store space specified by the supplied handle. Dropping of a store space may fail if there are
     * concurrent calls on the same store space or if the store space has already been dropped.
     *
     * @param handle of the store space
     * @throws SafeIllegalArgumentException if the {@code handle} points to a non-existing store space
     */
    void dropStoreSpace(PersistentStore.Handle handle);
}
