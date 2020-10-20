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
package com.palantir.atlasdb.cleaner;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * Wrap another PuncherStore, optimizing the #get() operation to operate on a local cache, only
 * invoking the underlying #get() on cache misses. To improve the cache hit rate, we round the wall
 * time to look up in the cache to a multiple of granularityMillis; this is safe because we always
 * round down.
 *
 * @author jweel
 */
public final class CachingPuncherStore implements PuncherStore {
    private static final int CACHE_SIZE = 64000;

    public static CachingPuncherStore create(final PuncherStore puncherStore, long granularityMillis) {
        LoadingCache<Long, Long> timeMillisToTimestamp = CacheBuilder.newBuilder()
                .maximumSize(CACHE_SIZE)
                .build(new CacheLoader<Long, Long>() {
                    @Override
                    public Long load(Long timeMillis) throws Exception {
                        return puncherStore.get(timeMillis);
                    }
                });
        return new CachingPuncherStore(puncherStore, timeMillisToTimestamp, granularityMillis);
    }

    private final PuncherStore puncherStore;
    private final LoadingCache<Long, Long> timeMillisToTimeStamp;
    private final long granularityMillis;

    private CachingPuncherStore(
            PuncherStore puncherStore, LoadingCache<Long, Long> timeMillisToTimestamp, long granularityMillis) {
        this.puncherStore = puncherStore;
        this.timeMillisToTimeStamp = timeMillisToTimestamp;
        this.granularityMillis = granularityMillis;
    }

    @Override
    public boolean isInitialized() {
        return puncherStore.isInitialized();
    }

    @Override
    public void put(long timestamp, long timeMillis) {
        puncherStore.put(timestamp, timeMillis);
    }

    @Override
    public Long get(Long timeMillis) {
        long approximateTimeMillis = timeMillis - (timeMillis % granularityMillis);
        return timeMillisToTimeStamp.getUnchecked(approximateTimeMillis);
    }

    @Override
    public long getMillisForTimestamp(long timestamp) {
        // Note: at the time of writing this (3-21-16), this operation is only
        // used optionally by the backup CLI and doesn't need to be cached
        return puncherStore.getMillisForTimestamp(timestamp);
    }
}
