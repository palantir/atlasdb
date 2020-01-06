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

package com.palantir.atlasdb.cache;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

import org.immutables.value.Value;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.off.heap.PersistentTimestampStore;
import com.palantir.atlasdb.off.heap.PersistentTimestampStore.StoreNamespace;

public final class OffHeapTimestampCache implements TimestampCache {
    private static final String TIMESTAMP_CACHE_NAMESPACE = "timestamp_cache";

    private final PersistentTimestampStore persistentTimestampStore;
    private final int maxSize;
    private final AtomicReference<CacheDescriptor> cacheDescriptor = new AtomicReference<>();
    private final ConcurrentHashMap<Long, Long> concurrentHashMap = new ConcurrentHashMap<>();

    public static TimestampCache create(PersistentTimestampStore persistentTimestampStore, int maxSize) {
        StoreNamespace storeNamespace = persistentTimestampStore.createNamespace(TIMESTAMP_CACHE_NAMESPACE);

        CacheDescriptor cacheDescriptor = ImmutableCacheDescriptor.builder()
                .currentSize(new AtomicInteger())
                .storeNamespace(storeNamespace)
                .build();

        return new OffHeapTimestampCache(persistentTimestampStore, cacheDescriptor, maxSize);
    }

    @VisibleForTesting
    OffHeapTimestampCache(
            PersistentTimestampStore persistentTimestampStore,
            CacheDescriptor cacheDescriptor,
            int maxSize) {
        this.persistentTimestampStore = persistentTimestampStore;
        this.cacheDescriptor.set(cacheDescriptor);
        this.maxSize = maxSize;
    }

    @Override
    public void clear() {
        CacheDescriptor proposedCacheDescriptor = constructCacheProposal(persistentTimestampStore);

        CacheDescriptor previous = cacheDescriptor.getAndUpdate(prev -> proposedCacheDescriptor);
        if (previous != null) {
            persistentTimestampStore.dropNamespace(previous.storeNamespace());
        }
    }


    @Override
    public void putAlreadyCommittedTransaction(Long startTimestamp, Long commitTimestamp) {
        // prevents concurrent writing of the same value
        if (concurrentHashMap.putIfAbsent(startTimestamp, commitTimestamp) != null) {
            return;
        }
        try {
            CacheDescriptor descriptor = cacheDescriptor.get();
            // if the value was already written by others back off
            if (persistentTimestampStore.get(descriptor.storeNamespace(), startTimestamp) != null) {
                return;
            }

            if (maxSize <= descriptor.currentSize().get()) {
                evictAllEntries();
            }

            descriptor = cacheDescriptor.get();
            persistentTimestampStore.put(descriptor.storeNamespace(), startTimestamp, commitTimestamp);
            descriptor.currentSize().incrementAndGet();
        } finally {
            concurrentHashMap.remove(startTimestamp);
        }
    }

    @Nullable
    @Override
    public Long getCommitTimestampIfPresent(Long startTimestamp) {
        // if another transaction is in the process of writing we can get the currently written value
        Long value = concurrentHashMap.get(startTimestamp);
        if (value != null) {
            return value;
        }

        return persistentTimestampStore.get(cacheDescriptor.get().storeNamespace(), startTimestamp);
    }

    private void evictAllEntries() {
        // Evict all entries can be called concurrently by multiple threads where only one should succeed in evicting
        // entries. If a cache is small this method will be called very frequently.
        CacheDescriptor currentDescriptor = cacheDescriptor.get();
        if (currentDescriptor.currentSize().get() < maxSize) {
            return;
        }

        CacheDescriptor proposedCacheDescriptor = constructCacheProposal(persistentTimestampStore);

        if (!cacheDescriptor.compareAndSet(currentDescriptor, proposedCacheDescriptor)) {
            persistentTimestampStore.dropNamespace(proposedCacheDescriptor.storeNamespace());
        } else {
            persistentTimestampStore.dropNamespace(currentDescriptor.storeNamespace());
        }
    }

    private static CacheDescriptor constructCacheProposal(PersistentTimestampStore persistentTimestampStore) {
        StoreNamespace proposal = persistentTimestampStore.createNamespace(TIMESTAMP_CACHE_NAMESPACE);
        return ImmutableCacheDescriptor.builder()
                .currentSize(new AtomicInteger())
                .storeNamespace(proposal)
                .build();
    }

    @Value.Immutable
    @Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE)
    public interface CacheDescriptor {
        AtomicInteger currentSize();
        StoreNamespace storeNamespace();
    }
}
