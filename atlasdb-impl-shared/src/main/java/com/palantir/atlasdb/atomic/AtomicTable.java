/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.atomic;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import java.util.Map;

/**
 * A generally persisted key value store that supports an atomic update operation.
 * @param <K> Key for the mapping
 * @param <V> Value for the mapping
 */
public interface AtomicTable<K, V> {
    default void markInProgress(K key) {
        markInProgress(ImmutableSet.of(key));
    }

    /**
     * Declares an operation for these keys is in progress. This method must be called before any
     * {@link #updateMultiple(Map)} for the key, or the latter might fail.
     */
    void markInProgress(Iterable<K> keys);

    /**
     * Atomic update. If the method does not throw, any subsequent get is guaranteed to return V. If the
     * method throws an exception, subsequent gets may return either V or previous value but once V is returned
     * subsequent calls are guaranteed to return V.
     */
    default void update(K key, V value) throws KeyAlreadyExistsException {
        updateMultiple(ImmutableMap.of(key, value));
    }

    /**
     * Similar to {@link AtomicTable#update(Object, Object)}, but may be implemented to batch
     * efficiently.
     */
    void updateMultiple(Map<K, V> keyValues) throws KeyAlreadyExistsException;

    default ListenableFuture<V> get(K key) {
        return Futures.transform(get(ImmutableList.of(key)), result -> result.get(key), MoreExecutors.directExecutor());
    }

    ListenableFuture<Map<K, V>> get(Iterable<K> keys);
}
