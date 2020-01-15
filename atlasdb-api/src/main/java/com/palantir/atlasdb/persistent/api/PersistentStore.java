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
import java.util.UUID;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.immutables.value.Value;

import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;

public interface PersistentStore extends AutoCloseable {
    @Value.Immutable
    interface StoreNamespace {
        String humanReadableName();
        UUID uniqueName();
    }

    /**
     * Gets the value associated with the entry specified by {@code key}.
     *
     * @param storeNamespace handle to the namespace from which we want to retrieve the value
     * @param key        entry key for which we want to retrieve the value
     * @return the associated value or null if the entry is missing
     * @throws com.palantir.logsafe.exceptions.SafeIllegalArgumentException when {@code storeNamespace} is a
     * handle to a non existing namespace
     */
    @Nullable
    byte[] get(StoreNamespace storeNamespace, @Nonnull byte[] key) throws SafeIllegalArgumentException;

    /**
     * Gets the values associated with the entries specified by {@code keys}.
     *
     * @param storeNamespace handle to the namespace from which we want to retrieve the values
     * @param keys representing keys for which we want to retrieve the values
     * @return a map from keys to values
     */
    Map<byte[], byte[]> multiGet(StoreNamespace storeNamespace, List<byte[]> keys);

    /**
     * Stores the {@code value} for the associated {@code key} while overwriting the existing value in the
     * specified {@code storeNamespace}.
     *
     * @param storeNamespace of the store to which we should store the entry
     * @param key        entry key
     * @param value       entry value
     * @throws com.palantir.logsafe.exceptions.SafeIllegalArgumentException when {@code storeNamespace} is a
     * handle to a non existing namespace
     */
    void put(StoreNamespace storeNamespace, @Nonnull byte[] key, @Nonnull byte[] value)
            throws SafeIllegalArgumentException;

    /**
     * Stores the entry pairs given in {@code toWrite}, overwriting the existing values.
     *
     * @param storeNamespace of the store to which we should store the entry
     * @param toWrite entry pairs to write
     * @throws com.palantir.logsafe.exceptions.SafeIllegalArgumentException when {@code storeNamespace} is a
     * handle to a non existing namespace
     */
    void multiPut(StoreNamespace storeNamespace, Map<byte[], byte[]> toWrite);

    /**
     * Creates a handle of type {@link StoreNamespace} with a {@link StoreNamespace#humanReadableName()} equals to
     * {@code name}. Multiple calls with the same supplied {@code name} will generate multiple namespaces. Users of
     * this API are required to keep uniqueness at the {@link StoreNamespace#humanReadableName()} level; otherwise,
     * multiple {@link StoreNamespace} backed by internal data structures will be maintained for the same AtlasDB
     * namespace, which is inefficient.
     *
     * @param name in human readable format of the namespace to be created
     * @return {@link StoreNamespace} which represents a handle to the created namespace
     */
    StoreNamespace createNamespace(@Nonnull String name);

    /**
     * Drops the namespace specified by the supplied handle. Dropping of a namespace may fail if there are
     * concurrent calls on the same namespace or if the namespace has already been dropped.
     *
     * @param storeNamespace handle
     * @throws com.palantir.logsafe.exceptions.SafeIllegalArgumentException if the supplied {@code storeNamespace} is a
     * handle to a non existing namespace
     */
    void dropNamespace(StoreNamespace storeNamespace) throws SafeIllegalArgumentException;
}
