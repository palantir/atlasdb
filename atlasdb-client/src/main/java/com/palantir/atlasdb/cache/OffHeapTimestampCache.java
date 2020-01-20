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
import com.palantir.atlasdb.persistent.api.PersistentStore;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.tritium.metrics.registry.MetricName;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

import okio.ByteString;

public final class OffHeapTimestampCache implements TimestampCache {
    private static final Logger log = LoggerFactory.getLogger(OffHeapTimestampCache.class);
    private static final String BATCHER_PURPOSE = "off-heap-timestamp-cache";
    private static final MetricName CACHE_HIT = constructCacheMetricName("cacheHit");
    private static final MetricName CACHE_MISS = constructCacheMetricName("cacheMiss");
    private static final MetricName CACHE_NUKE = constructCacheMetricName("cacheNuke");
    private static final MetricName CACHE_SIZE = constructCacheMetricName("cacheSize");

    private final TimestampStore timestampStore;
    private final LongSupplier maxSize;
    private final AtomicReference<CacheDescriptor> cacheDescriptor = new AtomicReference<>();
    private final TaggedMetricRegistry taggedMetricRegistry;
    private final ConcurrentMap<Long, Long> inflightRequests = new ConcurrentHashMap<>();
    private final DisruptorAutobatcher<Map.Entry<Long, Long>, Void> timestampPutter;

    public static TimestampCache create(
            PersistentStore<ByteString, ByteString> persistentStore,
            TaggedMetricRegistry taggedMetricRegistry,
            LongSupplier maxSize) {
        TimestampStore timestampStore = new TimestampStore(persistentStore);
        PersistentStore.Handle handle = timestampStore.createSpace();

        CacheDescriptor cacheDescriptor = ImmutableCacheDescriptor.builder()
                .currentSize(new AtomicInteger())
                .handle(handle)
                .build();

        return new OffHeapTimestampCache(
                timestampStore,
                cacheDescriptor,
                maxSize,
                taggedMetricRegistry);
    }

    private OffHeapTimestampCache(
            TimestampStore timestampStore,
            CacheDescriptor cacheDescriptor,
            LongSupplier maxSize,
            TaggedMetricRegistry taggedMetricRegistry) {
        this.timestampStore = timestampStore;
        this.cacheDescriptor.set(cacheDescriptor);
        this.maxSize = maxSize;
        this.taggedMetricRegistry = taggedMetricRegistry;
        this.timestampPutter = Autobatchers.coalescing(new WriteBatcher(this))
                .safeLoggablePurpose(BATCHER_PURPOSE)
                .build();
        Gauge<Integer> cacheSizeGauge = () -> OffHeapTimestampCache.this.cacheDescriptor.get().currentSize().intValue();
        taggedMetricRegistry.gauge(CACHE_SIZE, cacheSizeGauge);
    }

    @Override
    public void clear() {
        CacheDescriptor proposedCacheDescriptor = createNamespaceAndConstructCacheProposal(timestampStore);

        CacheDescriptor previous = cacheDescriptor.getAndUpdate(prev -> proposedCacheDescriptor);
        if (previous != null) {
            timestampStore.dropStoreSpace(previous.handle());
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
        Optional<Long> value = getCommitTimestamp(startTimestamp);

        if (value.isPresent()) {
            taggedMetricRegistry.meter(CACHE_HIT).mark();
        } else {
            taggedMetricRegistry.meter(CACHE_MISS).mark();
        }
        return value.orElse(null);
    }

    private Optional<Long> getCommitTimestamp(Long startTimestamp) {
        Long inFlight = inflightRequests.get(startTimestamp);
        if (inFlight != null) {
            return Optional.of(inFlight);
        }

        return timestampStore.get(cacheDescriptor.get().handle(), startTimestamp);
    }

    private static CacheDescriptor createNamespaceAndConstructCacheProposal(TimestampStore timestampStore) {
        PersistentStore.Handle proposal = timestampStore.createSpace();
        return ImmutableCacheDescriptor.builder()
                .currentSize(new AtomicInteger())
                .handle(proposal)
                .build();
    }

    private static MetricName constructCacheMetricName(String metricSuffix) {
        return MetricName.builder()
                .safeName(MetricRegistry.name(OffHeapTimestampCache.class, metricSuffix))
                .build();
    }

    private static class WriteBatcher implements CoalescingRequestFunction<Map.Entry<Long, Long>, Void> {
        OffHeapTimestampCache offHeapTimestampCache;

        WriteBatcher(OffHeapTimestampCache offHeapTimestampCache) {
            this.offHeapTimestampCache = offHeapTimestampCache;
        }

        @Override
        public Map<Map.Entry<Long, Long>, Void> apply(Set<Map.Entry<Long, Long>> request) {
            CacheDescriptor cacheDescriptor = offHeapTimestampCache.cacheDescriptor.get();
            if (cacheDescriptor.currentSize().get() >= offHeapTimestampCache.maxSize.getAsLong()) {
                offHeapTimestampCache.taggedMetricRegistry.counter(CACHE_NUKE).inc();
                offHeapTimestampCache.clear();
            }
            cacheDescriptor = offHeapTimestampCache.cacheDescriptor.get();
            try {
                List<Long> toWrite = request.stream().map(Map.Entry::getKey).collect(Collectors.toList());
                Map<Long, Long> response = offHeapTimestampCache.timestampStore.get(cacheDescriptor.handle(), toWrite);

                int sizeIncrease = Sets.difference(request, response.entrySet()).size();
                cacheDescriptor.currentSize().addAndGet(sizeIncrease);
                offHeapTimestampCache.timestampStore.put(cacheDescriptor.handle(), ImmutableMap.copyOf(request));
            } catch (SafeIllegalArgumentException exception) {
                // happens when a store is dropped by a concurrent call to clear
                log.warn("Clear called concurrently, writing failed", exception);
            } finally {
                offHeapTimestampCache.inflightRequests.clear();
            }
            return KeyedStream.of(request.stream()).<Void>map(value -> null).collectToMap();
        }
    }

    @Value.Immutable
    @Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE)
    interface CacheDescriptor {
        AtomicInteger currentSize();
        PersistentStore.Handle handle();
    }
}
