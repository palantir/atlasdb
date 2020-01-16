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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.palantir.atlasdb.autobatch.Autobatchers;
import com.palantir.atlasdb.autobatch.CoalescingRequestFunction;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher;
import com.palantir.atlasdb.persistent.api.PersistentStore;
import com.palantir.atlasdb.persistent.api.PersistentStore.StoreHandle;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.tritium.metrics.registry.MetricName;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

public final class DefaultOffHeapCache<K, V> implements OffHeapCache<K, V> {
    private static final Logger log = LoggerFactory.getLogger(DefaultOffHeapCache.class);
    private static final String BATCHER_PURPOSE = "off-heap-cache";
    private static final MetricName CACHE_HIT = constructCacheMetricName("cacheHit");
    private static final MetricName CACHE_MISS = constructCacheMetricName("cacheMiss");
    private static final MetricName CACHE_NUKE = constructCacheMetricName("cacheNuke");
    private static final MetricName CACHE_SIZE = constructCacheMetricName("cacheSize");

    private final PersistentStore<K, V> persistentStore;
    private final LongSupplier maxSize;
    private final AtomicReference<CacheDescriptor> cacheDescriptor = new AtomicReference<>();
    private final DisruptorAutobatcher<Map.Entry<K, V>, Void> valuePutter;
    private final Meter cacheHit;
    private final Meter cacheMiss;
    private final Counter cacheNuke;

    public static <K, V> OffHeapCache<K, V> create(
            PersistentStore<K, V> persistentStore,
            TaggedMetricRegistry taggedMetricRegistry,
            LongSupplier maxSize) {
        StoreHandle storeHandle = persistentStore.createStoreHandle();

        CacheDescriptor cacheDescriptor = ImmutableCacheDescriptor.builder()
                .currentSize(new AtomicInteger())
                .storeHandle(storeHandle)
                .build();

        return new DefaultOffHeapCache<>(
                persistentStore,
                cacheDescriptor,
                maxSize,
                taggedMetricRegistry);
    }

    DefaultOffHeapCache(
            PersistentStore<K, V> persistentStore,
            CacheDescriptor cacheDescriptor,
            LongSupplier maxSize,
            TaggedMetricRegistry taggedMetricRegistry) {
        this.persistentStore = persistentStore;
        this.cacheDescriptor.set(cacheDescriptor);
        this.maxSize = maxSize;
        this.cacheHit = taggedMetricRegistry.meter(CACHE_HIT);
        this.cacheMiss = taggedMetricRegistry.meter(CACHE_MISS);
        this.cacheNuke = taggedMetricRegistry.counter(CACHE_NUKE);
        this.valuePutter = Autobatchers.coalescing(new WriteBatcher<>(this))
                .safeLoggablePurpose(BATCHER_PURPOSE)
                .build();
        Gauge<Integer> cacheSizeGauge = () -> this.cacheDescriptor.get().currentSize().intValue();
        taggedMetricRegistry.gauge(CACHE_SIZE, cacheSizeGauge);
    }

    @Override
    public void clear() {
        CacheDescriptor proposedCacheDescriptor = createNamespaceAndConstructCacheProposal(persistentStore);

        CacheDescriptor previous = cacheDescriptor.getAndUpdate(prev -> proposedCacheDescriptor);
        if (previous != null) {
            persistentStore.dropStoreHandle(previous.storeHandle());
        }
    }

    @Override
    public void put(K key, V value) {
        Futures.getUnchecked(valuePutter.apply(Maps.immutableEntry(key, value)));
    }

    @Override
    public Optional<V> get(K key) {
        Optional<V> value = persistentStore.get(cacheDescriptor.get().storeHandle(), key);
        getCacheMeter(value.isPresent()).mark();
        return value;
    }

    private Meter getCacheMeter(boolean cacheOutcome) {
        return cacheOutcome ? cacheHit : cacheMiss;
    }

    private static <K, V> CacheDescriptor createNamespaceAndConstructCacheProposal(PersistentStore<K, V> store) {
        StoreHandle proposal = store.createStoreHandle();
        return ImmutableCacheDescriptor.builder()
                .currentSize(new AtomicInteger())
                .storeHandle(proposal)
                .build();
    }

    private static MetricName constructCacheMetricName(String metricSuffix) {
        return MetricName.builder()
                .safeName(MetricRegistry.name(DefaultOffHeapCache.class, metricSuffix))
                .build();
    }

    private static class WriteBatcher<K, V> implements CoalescingRequestFunction<Map.Entry<K, V>, Void> {
        DefaultOffHeapCache<K, V> offHeapCache;

        WriteBatcher(DefaultOffHeapCache<K, V> offHeapCache) {
            this.offHeapCache = offHeapCache;
        }

        @Override
        public Map<Map.Entry<K, V>, Void> apply(Set<Map.Entry<K, V>> request) {
            CacheDescriptor cacheDescriptor = offHeapCache.cacheDescriptor.get();
            if (cacheDescriptor.currentSize().get() >= offHeapCache.maxSize.getAsLong()) {
                offHeapCache.cacheNuke.inc();
                offHeapCache.clear();
            }
            cacheDescriptor = offHeapCache.cacheDescriptor.get();
            try {
                List<K> toWrite = request.stream().map(Map.Entry::getKey).collect(Collectors.toList());
                Map<K, V> response = offHeapCache.persistentStore.get(cacheDescriptor.storeHandle(), toWrite);

                int sizeIncrease = Sets.difference(request, response.entrySet()).size();
                offHeapCache.persistentStore.put(cacheDescriptor.storeHandle(), ImmutableMap.copyOf(request));

                cacheDescriptor.currentSize().addAndGet(sizeIncrease);
            } catch (SafeIllegalArgumentException exception) {
                // happens when a store is dropped by a concurrent call to clear
                log.warn("Clear called concurrently, writing failed", exception);
            }
            return KeyedStream.of(request).map(value -> (Void) null).collectToMap();
        }
    }

    @Value.Immutable
    @Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE)
    interface CacheDescriptor {
        AtomicInteger currentSize();
        StoreHandle storeHandle();
    }
}
