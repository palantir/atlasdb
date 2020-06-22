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
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.tritium.metrics.registry.MetricName;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import okio.ByteString;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DefaultOffHeapCache<K, V> implements OffHeapCache<K, V> {
    private static final Logger log = LoggerFactory.getLogger(DefaultOffHeapCache.class);
    private static final String BATCHER_PURPOSE = "off-heap-cache";
    private static final MetricName CACHE_HIT = constructCacheMetricName("cacheHit");
    private static final MetricName CACHE_MISS = constructCacheMetricName("cacheMiss");
    private static final MetricName CACHE_NUKE = constructCacheMetricName("cacheNuke");
    private static final MetricName CACHE_SIZE = constructCacheMetricName("cacheSize");

    private final PersistentStore persistentStore;
    private final EntryMapper<K, V> entryMapper;
    private final LongSupplier maxSize;
    private final AtomicReference<CacheDescriptor> cacheDescriptor = new AtomicReference<>();
    private final DisruptorAutobatcher<Map.Entry<K, V>, Void> valuePutter;
    private final Meter cacheHit;
    private final Meter cacheMiss;
    private final Counter cacheNuke;

    public interface EntryMapper<K, V> {
        ByteString serializeKey(K key);
        K deserializeKey(ByteString key);
        ByteString serializeValue(K key, V value);
        V deserializeValue(ByteString key, ByteString value);
    }

    public static <K, V> OffHeapCache<K, V> create(
            PersistentStore persistentStore,
            EntryMapper<K, V> entryMapper,
            TaggedMetricRegistry taggedMetricRegistry,
            LongSupplier maxSize) {
        PersistentStore.Handle handle = persistentStore.createSpace();

        CacheDescriptor cacheDescriptor = ImmutableCacheDescriptor.builder()
                .currentSize(new AtomicInteger())
                .handle(handle)
                .build();

        return new DefaultOffHeapCache(
                persistentStore,
                entryMapper,
                cacheDescriptor,
                maxSize,
                taggedMetricRegistry);
    }

    private DefaultOffHeapCache(
            PersistentStore persistentStore,
            EntryMapper<K, V> entryMapper,
            CacheDescriptor cacheDescriptor,
            LongSupplier maxSize,
            TaggedMetricRegistry taggedMetricRegistry) {
        this.persistentStore = persistentStore;
        this.entryMapper = entryMapper;
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
            persistentStore.dropStoreSpace(previous.handle());
        }
    }

    @Override
    public void put(K key, V value) {
        Futures.getUnchecked(valuePutter.apply(Maps.immutableEntry(key, value)));
    }

    @Override
    public Optional<V> get(K key) {
        ByteString serializedKey = entryMapper.serializeKey(key);
        Optional<ByteString> value = persistentStore.get(cacheDescriptor.get().handle(), serializedKey);
        getCacheMeter(value.isPresent()).mark();
        return value.map(v -> entryMapper.deserializeValue(serializedKey, v));
    }

    private Meter getCacheMeter(boolean cacheOutcome) {
        return cacheOutcome ? cacheHit : cacheMiss;
    }

    private static CacheDescriptor createNamespaceAndConstructCacheProposal(PersistentStore persistentStore) {
        PersistentStore.Handle proposal = persistentStore.createSpace();
        return ImmutableCacheDescriptor.builder()
                .currentSize(new AtomicInteger())
                .handle(proposal)
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
            Set<Map.Entry<ByteString, ByteString>> serializedRequest = request.stream()
                    .map(this::serializeEntry)
                    .collect(Collectors.toSet());
            try {
                List<ByteString> toWrite = serializedRequest.stream()
                        .map(Map.Entry::getKey)
                        .collect(Collectors.toList());
                Map<ByteString, ByteString> response =
                        offHeapCache.persistentStore.get(cacheDescriptor.handle(), toWrite);

                int sizeIncrease = Sets.difference(request, response.entrySet()).size();
                cacheDescriptor.currentSize().addAndGet(sizeIncrease);
                offHeapCache.persistentStore.put(
                        cacheDescriptor.handle(),
                        ImmutableMap.copyOf(serializedRequest));
            } catch (SafeIllegalArgumentException exception) {
                // happens when a store is dropped by a concurrent call to clear
                log.warn("Clear called concurrently, writing failed", exception);
            }
            return KeyedStream.of(request.stream()).<Void>map(value -> null).collectToMap();
        }

        private Map.Entry<ByteString, ByteString> serializeEntry(Map.Entry<K, V> entry) {
            return Maps.immutableEntry(
                    offHeapCache.entryMapper.serializeKey(entry.getKey()),
                    offHeapCache.entryMapper.serializeValue(entry.getKey(), entry.getValue()));
        }
    }

    @Value.Immutable
    @Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE)
    interface CacheDescriptor {
        AtomicInteger currentSize();
        PersistentStore.Handle handle();
    }
}
