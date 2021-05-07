/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.api.cache;

import com.palantir.atlasdb.keyvalue.api.watch.StartTimestamp;
import com.palantir.lock.watch.CommitUpdate;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
final class CacheStoreImpl implements CacheStore {
    private final SnapshotStore snapshotStore;
    private final Map<StartTimestamp, TransactionScopedCache> cacheMap;
    private final Map<StartTimestamp, TransactionScopedCache> readOnlyCacheMap;
    private final double validationProbability;

    CacheStoreImpl(SnapshotStore snapshotStore, double validationProbability) {
        this.snapshotStore = snapshotStore;
        this.cacheMap = new ConcurrentHashMap<>();
        this.validationProbability = validationProbability;
        this.readOnlyCacheMap = new ConcurrentHashMap<>();
    }

    @Override
    public TransactionScopedCache getOrCreateCache(StartTimestamp timestamp) {
        return cacheMap.computeIfAbsent(timestamp, key -> snapshotStore
                .getSnapshot(key)
                .map(TransactionScopedCacheImpl::create)
                .map(cache -> ValidatingTransactionScopedCache.create(cache, validationProbability))
                .orElseGet(NoOpTransactionScopedCache::create));
    }

    @Override
    public TransactionScopedCache getCache(StartTimestamp timestamp) {
        return getCacheInternal(timestamp).orElseGet(NoOpTransactionScopedCache::create);
    }

    @Override
    public void removeCache(StartTimestamp timestamp) {
        cacheMap.remove(timestamp);
        readOnlyCacheMap.remove(timestamp);
    }

    @Override
    public void reset() {
        cacheMap.clear();
        readOnlyCacheMap.clear();
    }

    @Override
    public void createReadOnlyCache(StartTimestamp timestamp, CommitUpdate commitUpdate) {
        getCacheInternal(timestamp)
                .map(cache -> cache.createReadOnlyCache(commitUpdate))
                .ifPresent(cache -> readOnlyCacheMap.put(timestamp, cache));
    }

    @Override
    public TransactionScopedCache getReadOnlyCache(StartTimestamp timestamp) {
        return Optional.ofNullable(readOnlyCacheMap.get(timestamp))
                .orElseGet(() -> NoOpTransactionScopedCache.create().createReadOnlyCache(CommitUpdate.invalidateAll()));
    }

    private Optional<TransactionScopedCache> getCacheInternal(StartTimestamp timestamp) {
        return Optional.ofNullable(cacheMap.get(timestamp));
    }
}
