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

import java.util.function.Supplier;

import javax.annotation.Nullable;

import com.codahale.metrics.MetricRegistry;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Policy;
import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.util.AtlasDbMetrics;

public class DefaultTimestampCache implements TimestampCache {
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

    public DefaultTimestampCache(MetricRegistry metricRegistry, Supplier<Long> size) {
        this.size = size;
        startToCommitTimestampCache = createCache(size.get());
        evictionPolicy = startToCommitTimestampCache.policy().eviction().get();
        AtlasDbMetrics.registerCache(metricRegistry, startToCommitTimestampCache,
                MetricRegistry.name(TimestampCache.class, "startToCommitTimestamp"));
    }

    @Override
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

    @Override
    public void putAlreadyCommittedTransaction(Long startTimestamp, Long commitTimestamp) {
        startToCommitTimestampCache.put(startTimestamp, commitTimestamp);
    }

    @Override
    public void clear() {
        startToCommitTimestampCache.invalidateAll();
    }

    public static TimestampCache createForTests() {
        return new DefaultTimestampCache(new MetricRegistry(), () -> 1000L);
    }
}
