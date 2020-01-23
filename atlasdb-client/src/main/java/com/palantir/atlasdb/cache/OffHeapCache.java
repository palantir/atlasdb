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

import java.util.Optional;

public interface OffHeapCache<K, V> {
    /**
     * Retrieves the value for the given {@code key}.
     *
     * @param key for which we want to get a value
     * @return optional containing the associated value or empty if the entry is not available
     */
    Optional<V> get(K key);

    /**
     * Caches entry pair.
     *
     * @param key   of the cached entry
     * @param value associated with a given key
     */
    void put(K key, V value);

    /**
     * Deletes all entries from the cache.
     */
    void clear();
}
