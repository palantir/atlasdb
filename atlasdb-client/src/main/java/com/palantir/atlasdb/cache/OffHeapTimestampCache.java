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
import java.util.Optional;
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

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableMap;
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
import com.palantir.tritium.metrics.registry.MetricName;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

public final class OffHeapTimestampCache implements TimestampCache {
    private static final Logger log = LoggerFactory.getLogger(OffHeapTimestampCache.class);
    private static final String TIMESTAMP_CACHE_NAMESPACE = "timestamp_cache";
    private static final String BATCHER_PURPOSE = "off-heap-timestamp-cache";
    private static final MetricName CACHE_HIT = constructCacheMetricName("cacheHit");
    private static final MetricName CACHE_MISS = constructCacheMetricName("cacheMiss");
    private static final MetricName CACHE_NUKE = constructCacheMetricName("cacheNuke");
    private static final MetricName CACHE_SIZE = constructCacheMetricName("cacheSize");

    private final PersistentTimestampStore persistentTimestampStore;
    private final int maxSize;
    private final AtomicReference<CacheDescriptor> cacheDescriptor = new AtomicReference<>();
    private final TaggedMetricRegistry taggedMetricRegistry;
    private final ConcurrentMap<Long, Long> inflightRequests = new ConcurrentHashMap<>();
    private final DisruptorAutobatcher<Map.Entry<Long, Long>, Map.Entry<Long, Long>> timestampPutter;
    private final ResettableCounter cacheSizeCounter;

    public static TimestampCache create(
            PersistentTimestampStore persistentTimestampStore,
            int maxSize,
            TaggedMetricRegistry taggedMetricRegistry) {
        StoreNamespace storeNamespace = persistentTimestampStore.createNamespace(TIMESTAMP_CACHE_NAMESPACE);

        CacheDescriptor cacheDescriptor = ImmutableCacheDescriptor.builder()
                .currentSize(new AtomicInteger())
                .storeNamespace(storeNamespace)
                .build();

        ResettableCounter resettableCounter = new ResettableCounter();
        taggedMetricRegistry.gauge(CACHE_SIZE, resettableCounter);

        return new OffHeapTimestampCache(
                persistentTimestampStore,
                cacheDescriptor,
                maxSize,
                taggedMetricRegistry,
                resettableCounter);
    }

    private OffHeapTimestampCache(
            PersistentTimestampStore persistentTimestampStore,
            CacheDescriptor cacheDescriptor,
            int maxSize,
            TaggedMetricRegistry taggedMetricRegistry,
            ResettableCounter resettableCounter) {
        this.persistentTimestampStore = persistentTimestampStore;
        this.cacheDescriptor.set(cacheDescriptor);
        this.maxSize = maxSize;
        this.taggedMetricRegistry = taggedMetricRegistry;
        this.timestampPutter = Autobatchers.coalescing(new WriteBatcher(this))
                .safeLoggablePurpose(BATCHER_PURPOSE)
                .build();
        this.cacheSizeCounter = resettableCounter;
    }

    @Override
    public void clear() {
        CacheDescriptor proposedCacheDescriptor = createNamespaceAndConstructCacheProposal(persistentTimestampStore);

        CacheDescriptor previous = cacheDescriptor.getAndUpdate(prev -> proposedCacheDescriptor);
        if (previous != null) {
            persistentTimestampStore.dropNamespace(previous.storeNamespace());
        }
    }


    @Override
    public void putAlreadyCommittedTransaction(Long startTimestamp, Long commitTimestamp) {
        if (inflightRequests.putIfAbsent(startTimestamp, commitTimestamp) != null) {
            return;
        }
        Futures.getUnchecked(timestampPutter.apply(Maps.immutableEntry(startTimestamp, commitTimestamp)));
    }

    @Nullable
    @Override
    public Long getCommitTimestampIfPresent(Long startTimestamp) {
        Long value = Optional.ofNullable(inflightRequests.get(startTimestamp))
                .orElseGet(() -> persistentTimestampStore.get(cacheDescriptor.get().storeNamespace(), startTimestamp));

        if (value == null) {
            taggedMetricRegistry.meter(CACHE_MISS).mark();
        } else {
            taggedMetricRegistry.meter(CACHE_HIT).mark();
        }
        return value;
    }

    private static CacheDescriptor createNamespaceAndConstructCacheProposal(
            PersistentTimestampStore persistentTimestampStore) {
        StoreNamespace proposal = persistentTimestampStore.createNamespace(TIMESTAMP_CACHE_NAMESPACE);
        return ImmutableCacheDescriptor.builder()
                .currentSize(new AtomicInteger())
                .storeNamespace(proposal)
                .build();
    }

    private static MetricName constructCacheMetricName(String metricSuffix) {
        return MetricName.builder()
                .safeName(MetricRegistry.name(OffHeapTimestampCache.class, metricSuffix))
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
                offHeapTimestampCache.taggedMetricRegistry.counter(CACHE_NUKE).inc();
                offHeapTimestampCache.cacheSizeCounter.reset();
                offHeapTimestampCache.clear();
            }
            CacheDescriptor cacheDescriptor = offHeapTimestampCache.cacheDescriptor.get();
            try {
                Map<Long, Long> response = offHeapTimestampCache.persistentTimestampStore.multiGet(
                        cacheDescriptor.storeNamespace(),
                        request.stream().map(Map.Entry::getKey).collect(Collectors.toList()));

                Map<Long, Long> toWrite = ImmutableMap.copyOf(Sets.difference(request, response.entrySet()));
                offHeapTimestampCache.persistentTimestampStore.multiPut(
                        cacheDescriptor.storeNamespace(),
                        toWrite);

                cacheDescriptor.currentSize().addAndGet(toWrite.size());
                offHeapTimestampCache.cacheSizeCounter.inc(toWrite.size());
            } catch (SafeIllegalArgumentException exception) {
                // happens when a store is dropped by a concurrent call to clear
                log.warn("Clear called concurrently, writing failed", exception);
            } finally {
                offHeapTimestampCache.inflightRequests.clear();
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
