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

    CacheStoreImpl(SnapshotStore snapshotStore) {
        this.snapshotStore = snapshotStore;
        this.cacheMap = new ConcurrentHashMap<>();
        readOnlyCacheMap = new ConcurrentHashMap<>();
    }

    @Override
    public Optional<TransactionScopedCache> createCache(StartTimestamp timestamp) {
        return snapshotStore
                .getSnapshot(timestamp)
                .map(TransactionScopedCacheImpl::create)
                .map(cache -> {
                    cacheMap.put(timestamp, cache);
                    return cache;
                });
    }

    @Override
    public Optional<TransactionScopedCache> getCache(StartTimestamp timestamp) {
        return Optional.ofNullable(cacheMap.get(timestamp));
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
    public void createReadOnlyCache(StartTimestamp startTimestamp, CommitUpdate commitUpdate) {
        Optional<TransactionScopedCache> transactionScopedCache =
                getCache(startTimestamp).map(cache -> cache.createReadOnlyCache(commitUpdate));
    }
}
