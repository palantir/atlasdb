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

import com.palantir.atlasdb.persistent.api.PersistentStore;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import java.util.function.LongSupplier;
import javax.annotation.Nullable;

public final class OffHeapTimestampCache implements TimestampCache {
    private final OffHeapCache<Long, Long> offHeapCache;

    public static TimestampCache create(
            PersistentStore persistentStore,
            TaggedMetricRegistry taggedMetricRegistry,
            LongSupplier maxSize) {
        return new OffHeapTimestampCache(
                DefaultOffHeapCache.create(
                        persistentStore,
                        new DeltaEncodingTimestampEntryMapper(new LongEntryMapper()),
                        taggedMetricRegistry,
                        maxSize));
    }

    private OffHeapTimestampCache(OffHeapCache<Long, Long> offHeapCache) {
        this.offHeapCache = offHeapCache;
    }

    @Override
    public void clear() {
        offHeapCache.clear();
    }

    @Override
    public void putAlreadyCommittedTransaction(Long startTimestamp, Long commitTimestamp) {
        offHeapCache.put(startTimestamp, commitTimestamp);
    }

    @Nullable
    @Override
    public Long getCommitTimestampIfPresent(Long startTimestamp) {
        return offHeapCache.get(startTimestamp).orElse(null);
    }
}
