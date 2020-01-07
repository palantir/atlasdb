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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.palantir.atlasdb.autobatch.Autobatchers;
import com.palantir.atlasdb.autobatch.CoalescingRequestFunction;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher;
import com.palantir.atlasdb.offheap.PersistentTimestampStore;
import com.palantir.atlasdb.offheap.PersistentTimestampStore.StoreNamespace;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;

public final class OffHeapTimestampCache implements TimestampCache {
    private static final String TIMESTAMP_CACHE_NAMESPACE = "timestamp_cache";
    private static final String BATCHER_PURPOSE = "off-heap-timestamp-cache";
    private static final Logger log = LoggerFactory.getLogger(OffHeapTimestampCache.class);

    private final PersistentTimestampStore persistentTimestampStore;
    private final int maxSize;
    private final AtomicReference<CacheDescriptor> cacheDescriptor = new AtomicReference<>();
    private final ConcurrentMap<Long, Long> concurrentHashMap = new ConcurrentHashMap<>();
    private final DisruptorAutobatcher<Map.Entry<Long, Long>, Map.Entry<Long, Long>> autobatcher;

    public static TimestampCache create(PersistentTimestampStore persistentTimestampStore, int maxSize) {
        StoreNamespace storeNamespace = persistentTimestampStore.createNamespace(TIMESTAMP_CACHE_NAMESPACE);

        CacheDescriptor cacheDescriptor = ImmutableCacheDescriptor.builder()
                .currentSize(new AtomicInteger())
                .storeNamespace(storeNamespace)
                .build();

        return new OffHeapTimestampCache(persistentTimestampStore, cacheDescriptor, maxSize);
    }

    private OffHeapTimestampCache(
            PersistentTimestampStore persistentTimestampStore,
            CacheDescriptor cacheDescriptor,
            int maxSize) {
        this.persistentTimestampStore = persistentTimestampStore;
        this.cacheDescriptor.set(cacheDescriptor);
        this.maxSize = maxSize;
        this.autobatcher = Autobatchers.coalescing(new WriteBatcher(this))
                .safeLoggablePurpose(BATCHER_PURPOSE)
                .build();
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
        if (concurrentHashMap.putIfAbsent(startTimestamp, commitTimestamp) != null) {
            return;
        }
        Futures.getUnchecked(autobatcher.apply(Maps.immutableEntry(startTimestamp, commitTimestamp)));
    }

    @Nullable
    @Override
    public Long getCommitTimestampIfPresent(Long startTimestamp) {
        Long value = concurrentHashMap.get(startTimestamp);
        if (value != null) {
            return value;
        }

        return persistentTimestampStore.get(cacheDescriptor.get().storeNamespace(), startTimestamp);
    }

    private static CacheDescriptor constructCacheProposal(PersistentTimestampStore persistentTimestampStore) {
        StoreNamespace proposal = persistentTimestampStore.createNamespace(TIMESTAMP_CACHE_NAMESPACE);
        return ImmutableCacheDescriptor.builder()
                .currentSize(new AtomicInteger())
                .storeNamespace(proposal)
                .build();
    }

    private static class WriteBatcher
            implements CoalescingRequestFunction<Map.Entry<Long, Long>, Map.Entry<Long, Long>> {
        OffHeapTimestampCache offHeapTimestampCache;

        WriteBatcher(OffHeapTimestampCache offHeapTimestampCache) {
            this.offHeapTimestampCache = offHeapTimestampCache;
        }

        @Override
        public Map<Map.Entry<Long, Long>, Map.Entry<Long, Long>> apply(Set<Map.Entry<Long, Long>> request) {
            if (offHeapTimestampCache.cacheDescriptor.get().currentSize().get() >= offHeapTimestampCache.maxSize) {
                offHeapTimestampCache.clear();
            }
            CacheDescriptor cacheDescriptor = offHeapTimestampCache.cacheDescriptor.get();
            try {
                Set<Map.Entry<Long, Long>> response = offHeapTimestampCache.persistentTimestampStore.multiGet(
                        cacheDescriptor.storeNamespace(),
                        request.stream().map(Map.Entry::getKey).collect(Collectors.toList()));

                Set<Map.Entry<Long, Long>> toWrite = Sets.difference(request, response);
                offHeapTimestampCache.persistentTimestampStore.multiPut(
                        cacheDescriptor.storeNamespace(),
                        toWrite);

                cacheDescriptor.currentSize().addAndGet(toWrite.size());
                offHeapTimestampCache.concurrentHashMap.clear();
            } catch (SafeIllegalArgumentException exception) {
                log.warn("Clear called concurrently, writing failed");
            }
            return KeyedStream.of(request.stream()).collectToMap();
        }
    }

    @Value.Immutable
    @Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE)
    public interface CacheDescriptor {
        AtomicInteger currentSize();
        StoreNamespace storeNamespace();
    }
}
