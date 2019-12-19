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

package com.palantir.atlasdb.local.storage.api;

import java.util.Collection;
import java.util.SortedMap;
import java.util.UUID;

import org.immutables.value.Value;

public interface PersistentStore extends AutoCloseable {
    @Value.Immutable
    interface StoreNamespace<T extends Comparable<T>, R> {
        String humanReadableName();
        UUID uniqueName();
        Serializer<T, R> serializer();
    }

    interface Serializer<T extends Comparable<T>, R> {
        byte[] serializeKey(T key);
        T deserializeKey(byte[] key);
        byte[] serializeValue(T key, R value);
        R deserializeValue(T key, byte[] value);
    }

    /**
     * Gets the value associated with the entry specified by {@code key}.
     *
     * @param storeNamespace handle to the namespace from which we want to retrieve the value
     * @param key        entry key for which we want to retrieve value
     * @return the associated value or null if the entry is missing
     * @throws com.palantir.logsafe.exceptions.SafeIllegalArgumentException when {@code storeNamespace} is a
     * handle to a non existing namespace
     */
    <K extends Comparable<K>, V> V get(StoreNamespace<K, V> storeNamespace, K key);

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
    <K extends Comparable<K>, V> void put(StoreNamespace<K, V> storeNamespace, K key, V value);

    /**
     * Creates a handle of type {@link StoreNamespace} with a {@link StoreNamespace#humanReadableName()} equals to
     * name. Multiple calls with the same supplied {@code name} will generate multiple namespaces. Users of this API are
     * required to keep uniqueness at the {@link StoreNamespace#humanReadableName()} level.
     *
     * @param name in human readable format of the namespace to be created
     * @return {@link StoreNamespace} which represents a handle to the created namespace
     */
    <K extends Comparable<K>, V> StoreNamespace<K, V> createNamespace(String name, Serializer<K, V> serializer);

    /**
     * Drops the namespace specified by the supplied handle. Dropping of a namespace may fail if called there are
     * concurrent calls on the same namespace or if the namespace has already been dropped.
     *
     * @param storeNamespace handle
     * @throws com.palantir.logsafe.exceptions.SafeIllegalArgumentException if the supplied {@code storeNamespace} is a
     * handle to a non existing namespace
     */
    void dropNamespace(StoreNamespace<?, ?> storeNamespace);

    /**
     * Loads all keys stored in the {@code storeNamespace}
     *
     * @param storeNamespace for which to retrieve the data
     * @return all the keys for the given {@link StoreNamespace} or empty collection if the namespace is empty
     */
    <K extends Comparable<K>> Collection<K> loadNamespaceKeys(StoreNamespace<K, ?> storeNamespace);

    /**
     * Loads all entries for the {@code storeNamespace}.
     *
     * @param storeNamespace for which to load entries
     * @return {@link SortedMap} of the entries in storage
     */
    <K extends Comparable<K>, V> SortedMap<K, V> loadNamespaceEntries(StoreNamespace<K, V> storeNamespace);
}
