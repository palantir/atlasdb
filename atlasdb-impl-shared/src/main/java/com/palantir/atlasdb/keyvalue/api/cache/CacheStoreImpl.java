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
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.concurrent.ThreadSafe;
import org.immutables.value.Value;

@ThreadSafe
final class CacheStoreImpl implements CacheStore {
    private static final SafeLogger log = SafeLoggerFactory.get(CacheStoreImpl.class);

    private final int maxCacheCount;
    private final SnapshotStore snapshotStore;
    private final Map<StartTimestamp, Caches> cacheMap;
    private final double validationProbability;
    private final Runnable failureCallback;
    private final CacheMetrics metrics;

    CacheStoreImpl(
            SnapshotStore snapshotStore,
            double validationProbability,
            Runnable failureCallback,
            CacheMetrics metrics,
            int maxCacheCount) {
        this.snapshotStore = snapshotStore;
        this.failureCallback = failureCallback;
        this.metrics = metrics;
        this.maxCacheCount = maxCacheCount;
        this.cacheMap = new ConcurrentHashMap<>();
        this.validationProbability = validationProbability;
        metrics.setTransactionCacheInstanceCountGauge(cacheMap::size);
    }

    @Override
    public void createCache(StartTimestamp timestamp) {
        validateStateSize();

        cacheMap.computeIfAbsent(timestamp, key -> snapshotStore
                .getSnapshot(key)
                .map(snapshot -> TransactionScopedCacheImpl.create(snapshot, metrics))
                .map(newCache -> ValidatingTransactionScopedCache.create(
                        newCache, validationProbability, failureCallback, metrics))
                .map(Caches::create)
                .orElse(null));
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
        log.info("Clearing all cache state");
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

    private void validateStateSize() {
        if (cacheMap.size() > maxCacheCount) {
            log.warn(
                    "Transaction cache store has exceeded maximum concurrent caches. This likely indicates a memory"
                            + " leak",
                    SafeArg.of("cacheMapSize", cacheMap.size()),
                    SafeArg.of("maxCacheCount", maxCacheCount),
                    SafeArg.of("earliestTimestamp", cacheMap.keySet().stream().min(Comparator.naturalOrder())));
            throw new SafeIllegalStateException(
                    "Exceeded maximum concurrent caches; transaction can be retried, but with caching disabled");
        }
    }

    private Optional<Caches> getCacheInternal(StartTimestamp timestamp) {
        return Optional.ofNullable(cacheMap.get(timestamp));
    }

    @Value.Immutable
    interface Caches {
        TransactionScopedCache mainCache();

        Optional<TransactionScopedCache> readOnlyCache();

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
