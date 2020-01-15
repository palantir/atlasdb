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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

import com.palantir.atlasdb.persistent.api.LogicalPersistentStore;
import com.palantir.atlasdb.persistent.api.PersistentStore.StoreNamespace;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

public final class OffHeapCaches {
    static final String TIMESTAMP_CACHE_NAMESPACE = "default_off_heap_timestamp_cache";

    private OffHeapCaches() {
    }

    public static <K, V> OffHeapCache<K, V> create(
            LogicalPersistentStore<K, V> persistentStore,
            TaggedMetricRegistry taggedMetricRegistry,
            LongSupplier maxSize) {
        StoreNamespace storeNamespace = persistentStore.createNamespace(TIMESTAMP_CACHE_NAMESPACE);

        DefaultOffHeapCache.CacheDescriptor cacheDescriptor = ImmutableCacheDescriptor.builder()
                .currentSize(new AtomicInteger())
                .storeNamespace(storeNamespace)
                .build();

        return new DefaultOffHeapCache<>(
                persistentStore,
                cacheDescriptor,
                maxSize,
                taggedMetricRegistry);
    }
}
