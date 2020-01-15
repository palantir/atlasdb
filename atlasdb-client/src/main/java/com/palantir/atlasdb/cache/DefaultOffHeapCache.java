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

import static com.palantir.atlasdb.cache.OffHeapCaches.TIMESTAMP_CACHE_NAMESPACE;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.palantir.atlasdb.autobatch.Autobatchers;
import com.palantir.atlasdb.autobatch.CoalescingRequestFunction;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher;
import com.palantir.atlasdb.persistent.api.LogicalPersistentStore;
import com.palantir.atlasdb.persistent.api.PersistentStore.StoreNamespace;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.tritium.metrics.registry.MetricName;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

final class DefaultOffHeapCache<K, V> implements OffHeapCache<K, V> {
    private static final Logger log = LoggerFactory.getLogger(DefaultOffHeapCache.class);
    private static final String BATCHER_PURPOSE = "off-heap-timestamp-cache";
    private static final MetricName CACHE_HIT = constructCacheMetricName("cacheHit");
    private static final MetricName CACHE_MISS = constructCacheMetricName("cacheMiss");
    private static final MetricName CACHE_NUKE = constructCacheMetricName("cacheNuke");
    private static final MetricName CACHE_SIZE = constructCacheMetricName("cacheSize");

    private final LogicalPersistentStore<K, V> persistentStore;
    private final LongSupplier maxSize;
    private final AtomicReference<CacheDescriptor> cacheDescriptor = new AtomicReference<>();
    private final TaggedMetricRegistry taggedMetricRegistry;
    private final ConcurrentMap<K, V> inflightRequests = new ConcurrentHashMap<>();
    private final DisruptorAutobatcher<Map.Entry<K, V>, Map.Entry<K, V>> timestampPutter;

    DefaultOffHeapCache(
            LogicalPersistentStore<K, V> persistentStore,
            CacheDescriptor cacheDescriptor,
            LongSupplier maxSize,
            TaggedMetricRegistry taggedMetricRegistry) {
        this.persistentStore = persistentStore;
        this.cacheDescriptor.set(cacheDescriptor);
        this.maxSize = maxSize;
        this.taggedMetricRegistry = taggedMetricRegistry;
        this.timestampPutter = Autobatchers.coalescing(new WriteBatcher<>(this))
                .safeLoggablePurpose(BATCHER_PURPOSE)
                .build();
        Gauge<Integer> cacheSizeGauge = () -> DefaultOffHeapCache.this.cacheDescriptor.get().currentSize().intValue();
        taggedMetricRegistry.gauge(CACHE_SIZE, cacheSizeGauge);
    }

    public void clear() {
        CacheDescriptor proposedCacheDescriptor = createNamespaceAndConstructCacheProposal(persistentStore);

        CacheDescriptor previous = cacheDescriptor.getAndUpdate(prev -> proposedCacheDescriptor);
        if (previous != null) {
            persistentStore.dropNamespace(previous.storeNamespace());
        }
    }

    @Override
    public void put(K startTimestamp, V commitTimestamp) {
        if (inflightRequests.putIfAbsent(startTimestamp, commitTimestamp) != null) {
            return;
        }
        Futures.getUnchecked(timestampPutter.apply(Maps.immutableEntry(startTimestamp, commitTimestamp)));
    }

    @Nullable
    public V get(K startTimestamp) {
        V value = Optional.ofNullable(inflightRequests.get(startTimestamp))
                .orElseGet(() -> persistentStore.get(cacheDescriptor.get().storeNamespace(), startTimestamp));

        if (value == null) {
            taggedMetricRegistry.meter(CACHE_MISS).mark();
        } else {
            taggedMetricRegistry.meter(CACHE_HIT).mark();
        }
        return value;
    }

    private static CacheDescriptor createNamespaceAndConstructCacheProposal(
            LogicalPersistentStore<?, ?> timestampStore) {
        StoreNamespace proposal = timestampStore.createNamespace(TIMESTAMP_CACHE_NAMESPACE);
        return ImmutableCacheDescriptor.builder()
                .currentSize(new AtomicInteger())
                .storeNamespace(proposal)
                .build();
    }

    private static MetricName constructCacheMetricName(String metricSuffix) {
        return MetricName.builder()
                .safeName(MetricRegistry.name(DefaultOffHeapCache.class, metricSuffix))
                .build();
    }

    private static class WriteBatcher <K, V>
            implements CoalescingRequestFunction<Map.Entry<K, V>, Map.Entry<K, V>> {
        DefaultOffHeapCache<K, V> offHeapCache;

        WriteBatcher(DefaultOffHeapCache<K, V> offHeapCache) {
            this.offHeapCache = offHeapCache;
        }

        @Override
        public Map<Map.Entry<K, V>, Map.Entry<K, V>> apply(Set<Map.Entry<K, V>> request) {
            CacheDescriptor cacheDescriptor = offHeapCache.cacheDescriptor.get();
            if (cacheDescriptor.currentSize().get() >= offHeapCache.maxSize.getAsLong()) {
                offHeapCache.taggedMetricRegistry.counter(CACHE_NUKE).inc();
                offHeapCache.clear();
            }
            cacheDescriptor = offHeapCache.cacheDescriptor.get();
            try {
                Map<K, V> response = offHeapCache.persistentStore.multiGet(
                        cacheDescriptor.storeNamespace(),
                        request.stream().map(Map.Entry::getKey).collect(Collectors.toList()));

                int sizeIncrease = Sets.difference(request, response.entrySet()).size();
                offHeapCache.persistentStore.multiPut(
                        cacheDescriptor.storeNamespace(),
                        ImmutableMap.copyOf(request));

                cacheDescriptor.currentSize().addAndGet(sizeIncrease);
            } catch (SafeIllegalArgumentException exception) {
                // happens when a store is dropped by a concurrent call to clear
                log.warn("Clear called concurrently, writing failed", exception);
            } finally {
                offHeapCache.inflightRequests.clear();
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
