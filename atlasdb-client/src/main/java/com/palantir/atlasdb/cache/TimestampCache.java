/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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

import javax.annotation.Nullable;

import com.codahale.metrics.MetricRegistry;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Policy;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.palantir.atlasdb.util.AtlasDbMetrics;

public class TimestampCache {
    private final Supplier<Long> size;

    private final Cache<Long, Long> startToCommitTimestampCache;
    private final Policy.Eviction<Long, Long> evictionPolicy;

    @VisibleForTesting
    static Cache<Long, Long> createCache(long size) {
        return Caffeine.newBuilder()
                .maximumSize(size)
                .recordStats()
                .build();
    }

    public TimestampCache(MetricRegistry metricRegistry, Supplier<Long> size) {
        this.size = size;
        startToCommitTimestampCache = createCache(size.get());
        evictionPolicy = startToCommitTimestampCache.policy().eviction().get();
        AtlasDbMetrics.registerCache(metricRegistry, startToCommitTimestampCache,
                MetricRegistry.name(TimestampCache.class, "startToCommitTimestamp"));
    }

    @VisibleForTesting
    TimestampCache(Cache<Long, Long> cache) {
        this.evictionPolicy = cache.policy().eviction().get();
        this.size = evictionPolicy::getMaximum;
        this.startToCommitTimestampCache = cache;
    }

    /**
     * Returns null if not present.
     *
     * @param startTimestamp transaction start timestamp
     * @return commit timestamp for the specified transaction start timestamp if present in cache, otherwise null
     */
    @Nullable
    public Long getCommitTimestampIfPresent(Long startTimestamp) {
        resizeIfNecessary();
        return startToCommitTimestampCache.getIfPresent(startTimestamp);
    }

    private void resizeIfNecessary() {
        if (evictionPolicy.getMaximum() != size.get()) {
            evictionPolicy.setMaximum(size.get());
        }
    }

    /**
     * Be very careful to only insert timestamps here that are already present in the backing store,
     * effectively using the timestamp table as existing concurrency control for who wins a commit.
     *
     * @param startTimestamp transaction start timestamp
     * @param commitTimestamp transaction commit timestamp
     */
    public void putAlreadyCommittedTransaction(Long startTimestamp, Long commitTimestamp) {
        startToCommitTimestampCache.put(startTimestamp, commitTimestamp);
    }

    /**
     * Clear all values from the cache.
     */
    public void clear() {
        startToCommitTimestampCache.invalidateAll();
    }

    public static TimestampCache createForTests() {
        return new TimestampCache(new MetricRegistry(), () -> 1000L);
    }
}
