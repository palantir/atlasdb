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
import com.palantir.logsafe.Preconditions;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.concurrent.ThreadSafe;
import org.immutables.value.Value;

@ThreadSafe
final class CacheStoreImpl implements CacheStore {
    private final SnapshotStore snapshotStore;
    private final Map<StartTimestamp, Caches> cacheMap;
    private final double validationProbability;

    CacheStoreImpl(SnapshotStore snapshotStore, double validationProbability) {
        this.snapshotStore = snapshotStore;
        this.cacheMap = new ConcurrentHashMap<>();
        this.validationProbability = validationProbability;
    }

    @Override
    public TransactionScopedCache getOrCreateCache(StartTimestamp timestamp) {
        return cacheMap.computeIfAbsent(timestamp, key -> snapshotStore
                        .getSnapshot(key)
                        .map(TransactionScopedCacheImpl::create)
                        .map(cache -> ValidatingTransactionScopedCache.create(cache, validationProbability))
                        .map(Caches::create)
                        .orElseGet(Caches::createNoOp))
                .mainCache();
    }

    @Override
    public TransactionScopedCache getCache(StartTimestamp timestamp) {
        return getCacheInternal(timestamp).map(Caches::mainCache).orElseGet(NoOpTransactionScopedCache::create);
    }

    @Override
    public void removeCache(StartTimestamp timestamp) {
        cacheMap.remove(timestamp);
    }

    @Override
    public void reset() {
        cacheMap.clear();
    }

    @Override
    public void createReadOnlyCache(StartTimestamp timestamp, CommitUpdate commitUpdate) {
        cacheMap.computeIfPresent(timestamp, (_startTs, cache) -> cache.withReadOnlyCache(commitUpdate));
    }

    @Override
    public TransactionScopedCache getReadOnlyCache(StartTimestamp timestamp) {
        return getCacheInternal(timestamp)
                .flatMap(Caches::readOnlyCache)
                .orElseGet(() -> NoOpTransactionScopedCache.create().createReadOnlyCache(CommitUpdate.invalidateAll()));
    }

    private Optional<Caches> getCacheInternal(StartTimestamp timestamp) {
        return Optional.ofNullable(cacheMap.get(timestamp));
    }

    @Value.Immutable
    interface Caches {
        TransactionScopedCache mainCache();

        Optional<TransactionScopedCache> readOnlyCache();

        static Caches createNoOp() {
            return create(NoOpTransactionScopedCache.create());
        }

        static Caches create(TransactionScopedCache mainCache) {
            return ImmutableCaches.builder().mainCache(mainCache).build();
        }

        default Caches withReadOnlyCache(CommitUpdate commitUpdate) {
            Preconditions.checkState(!readOnlyCache().isPresent(), "Read-only cache is already present");
            return ImmutableCaches.builder()
                    .from(this)
                    .readOnlyCache(mainCache().createReadOnlyCache(commitUpdate))
                    .build();
        }
    }
}
