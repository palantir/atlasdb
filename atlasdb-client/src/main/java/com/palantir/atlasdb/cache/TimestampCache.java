/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.palantir.atlasdb.util.AtlasDbMetrics;

/**
 * This class just here for readability and not directly leaking / tying us down to a Guava class in our API.
 */
public class TimestampCache {

    private final Cache<Long, Long> startToCommitTimestampCache;

    public static TimestampCache create() {
        TimestampCache timestampCache = new TimestampCache(createDefaultCache());
        AtlasDbMetrics.registerCache(timestampCache.startToCommitTimestampCache,
                MetricRegistry.name(TimestampCache.class, "startToCommitTimestamp"));
        return timestampCache;
    }

    @VisibleForTesting
    TimestampCache(Cache<Long, Long> cache) {
        this.startToCommitTimestampCache = cache;
    }

    @VisibleForTesting
    static Cache<Long, Long> createDefaultCache() {
        return CacheBuilder.newBuilder()
                .maximumSize(1_000_000) // up to ~72MB with java Long object bloat
                .recordStats()
                .build();
    }

    /**
     * Returns null if not present.
     *
     * @param startTimestamp transaction start timestamp
     * @return commit timestamp for the specified transaction start timestamp if present in cache, otherwise null
     */
    @Nullable
    public Long getCommitTimestampIfPresent(Long startTimestamp) {
        return startToCommitTimestampCache.getIfPresent(startTimestamp);
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
}
