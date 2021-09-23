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

package com.palantir.atlasdb.keyvalue.api.watch;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.keyvalue.api.LockWatchCachingConfig;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.cache.CacheMetrics;
import com.palantir.atlasdb.keyvalue.api.cache.LockWatchValueScopingCache;
import com.palantir.atlasdb.keyvalue.api.cache.LockWatchValueScopingCacheImpl;
import com.palantir.atlasdb.keyvalue.api.cache.TransactionScopedCache;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.timelock.api.LockWatchRequest;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.lock.client.NamespacedConjureLockWatchingService;
import com.palantir.lock.watch.*;
import com.palantir.lock.watch.LockWatchReferences.LockWatchReference;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public final class LockWatchManagerImpl extends LockWatchManagerInternal {
    private static final SafeLogger log = SafeLoggerFactory.get(LockWatchManagerImpl.class);

    private final Set<LockWatchReferences.LockWatchReference> referencesFromSchema;
    private final Set<LockWatchReferences.LockWatchReference> lockWatchReferences = ConcurrentHashMap.newKeySet();
    private final LockWatchCache lockWatchCache;
    private final LockWatchValueScopingCache valueScopingCache;
    private final NamespacedConjureLockWatchingService lockWatchingService;
    private final ScheduledExecutorService executorService = PTExecutors.newSingleThreadScheduledExecutor();
    private final ScheduledFuture<?> refreshTask;

    @VisibleForTesting
    LockWatchManagerImpl(
            Set<LockWatchReference> referencesFromSchema,
            LockWatchEventCache eventCache,
            LockWatchValueScopingCache valueCache,
            NamespacedConjureLockWatchingService lockWatchingService) {
        this.referencesFromSchema = referencesFromSchema;
        this.lockWatchCache = new LockWatchCacheImpl(eventCache, valueCache);
        this.valueScopingCache = valueCache;
        this.lockWatchingService = lockWatchingService;
        lockWatchReferences.addAll(referencesFromSchema);
        refreshTask = executorService.scheduleWithFixedDelay(this::registerWatchesWithTimelock, 0, 5, TimeUnit.SECONDS);
    }

    public static LockWatchManagerInternal create(
            MetricsManager metricsManager,
            Set<Schema> schemas,
            NamespacedConjureLockWatchingService lockWatchingService,
            LockWatchCachingConfig config) {
        Set<LockWatchReference> referencesFromSchema = schemas.stream()
                .map(Schema::getLockWatches)
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
        Set<TableReference> watchedTablesFromSchema = referencesFromSchema.stream()
                .map(schema -> schema.accept(LockWatchReferencesVisitor.INSTANCE))
                .collect(Collectors.toSet());
        CacheMetrics metrics = CacheMetrics.create(metricsManager);
        LockWatchEventCache eventCache = LockWatchEventCacheImpl.create(metrics);
        LockWatchValueScopingCache valueCache = LockWatchValueScopingCacheImpl.create(
                eventCache, metrics, config.cacheSize(), config.validationProbability(), watchedTablesFromSchema);
        return new LockWatchManagerImpl(referencesFromSchema, eventCache, valueCache, lockWatchingService);
    }

    @Override
    CommitUpdate getCommitUpdate(long startTs) {
        return lockWatchCache.getEventCache().getCommitUpdate(startTs);
    }

    @Override
    TransactionsLockWatchUpdate getUpdateForTransactions(
            Set<Long> startTimestamps, Optional<LockWatchVersion> version) {
        return lockWatchCache.getEventCache().getUpdateForTransactions(startTimestamps, version);
    }

    @Override
    public void close() {
        refreshTask.cancel(false);
        executorService.shutdown();
    }

    @Override
    public void registerPreciselyWatches(Set<LockWatchReferences.LockWatchReference> newLockWatches) {
        lockWatchReferences.clear();
        lockWatchReferences.addAll(referencesFromSchema);
        lockWatchReferences.addAll(newLockWatches);
        registerWatchesWithTimelock();
    }

    @Override
    boolean isEnabled() {
        return lockWatchCache.getEventCache().isEnabled();
    }

    @Override
    public LockWatchCache getCache() {
        return lockWatchCache;
    }

    @Override
    public void removeTransactionStateFromCache(long startTs) {
        lockWatchCache.removeTransactionStateFromCache(startTs);
    }

    @Override
    public void onSuccess(long startTs) {
        lockWatchCache.onSuccess(startTs);
    }

    @Override
    public TransactionScopedCache getTransactionScopedCache(long startTs) {
        return valueScopingCache.getTransactionScopedCache(startTs);
    }

    @Override
    public TransactionScopedCache getReadOnlyTransactionScopedCache(long startTs) {
        return valueScopingCache.getReadOnlyTransactionScopedCacheForCommit(startTs);
    }

    private void registerWatchesWithTimelock() {
        if (lockWatchReferences.isEmpty()) {
            return;
        }

        try {
            lockWatchingService.startWatching(LockWatchRequest.of(lockWatchReferences));
        } catch (Throwable e) {
            log.info("Failed to register lockwatches", UnsafeArg.of("lockwatches", lockWatchReferences), e);
        }
    }
}
