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

package com.palantir.atlasdb.off.heap;

import java.util.UUID;

import org.immutables.value.Value;

public interface PersistentTimestampStore extends AutoCloseable {
    @Value.Immutable
    interface StoreNamespace {
        String humanReadableName();
        UUID uniqueName();
    }

    /**
     * Gets the commit timestamp associated with the entry specified by {@code startTs}.
     *
     * @param storeNamespace handle to the namespace from which we want to retrieve the commit timestamp
     * @param startTs        entry key for which we want to retrieve commit timestamp
     * @return the associated timestamp or null if the entry is missing
     * @throws com.palantir.logsafe.exceptions.SafeIllegalArgumentException when {@code storeNamespace} is a
     * handle to a non existing namespace
     */
    Long get(StoreNamespace storeNamespace, Long startTs);

    /**
     * Stores the {@code commitTs} for the associated {@code startTs} while overwriting the existing value in the
     * specified {@code storeNamespace}.
     *
     * @param storeNamespace of the store to which we should store the entry
     * @param startTs        entry key
     * @param commitTs       entry value
     * @throws com.palantir.logsafe.exceptions.SafeIllegalArgumentException when {@code storeNamespace} is a
     * handle to a non existing namespace
     */
    void put(StoreNamespace storeNamespace, Long startTs, Long commitTs);

    /**
     * Creates a handle of type {@link StoreNamespace} with a {@link StoreNamespace#humanReadableName()} equals to
     * name. Multiple calls with the same supplied {@code name} will generate multiple namespaces. Users of this API are
     * required to keep uniqueness at the {@link StoreNamespace#humanReadableName()} level.
     *
     * @param name in human readable format of the namespace to be created
     * @return {@link StoreNamespace} which represents a handle to the created namespace
     */
    StoreNamespace createNamespace(String name);

    /**
     * Drops the namespace specified by the supplied handle. Dropping of a namespace may fail if there are
     * concurrent calls on the same namespace or if the namespace has already been dropped.
     *
     * @param storeNamespace handle
     * @throws com.palantir.logsafe.exceptions.SafeIllegalArgumentException if the supplied {@code storeNamespace} is a
     * handle to a non existing namespace
     */
    void dropNamespace(StoreNamespace storeNamespace);
}
