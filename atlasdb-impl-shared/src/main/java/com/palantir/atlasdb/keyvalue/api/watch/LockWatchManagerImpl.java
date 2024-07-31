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
import com.palantir.lock.client.LockWatchStarter;
import com.palantir.lock.watch.CommitUpdate;
import com.palantir.lock.watch.LockWatchCache;
import com.palantir.lock.watch.LockWatchCacheImpl;
import com.palantir.lock.watch.LockWatchEventCache;
import com.palantir.lock.watch.LockWatchReferenceTableExtractor;
import com.palantir.lock.watch.LockWatchReferences;
import com.palantir.lock.watch.LockWatchReferences.LockWatchReference;
import com.palantir.lock.watch.LockWatchVersion;
import com.palantir.lock.watch.TransactionsLockWatchUpdate;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.util.RateLimitedLogger;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public final class LockWatchManagerImpl extends LockWatchManagerInternal {
    private static final SafeLogger log = SafeLoggerFactory.get(LockWatchManagerImpl.class);

    // Log at most 1 line every 2 minutes. Diagnostics are expected to be triggered
    // on exceptional circumstances and a one-off basis. This de-duplicates when we
    // need diagnostics on large clusters.
    private static final RateLimitedLogger diagnosticLog = new RateLimitedLogger(log, 1 / 120.0);

    private final Set<LockWatchReferences.LockWatchReference> referencesFromSchema;
    private final Set<LockWatchReferences.LockWatchReference> lockWatchReferences = ConcurrentHashMap.newKeySet();
    private final LockWatchCache lockWatchCache;
    private final LockWatchValueScopingCache valueScopingCache;
    private final LockWatchStarter lockWatchingService;
    private final ScheduledExecutorService executorService = PTExecutors.newSingleThreadScheduledExecutor();
    private final ScheduledFuture<?> refreshTask;

    @VisibleForTesting
    LockWatchManagerImpl(
            Set<LockWatchReference> referencesFromSchema,
            LockWatchEventCache eventCache,
            LockWatchValueScopingCache valueCache,
            LockWatchStarter lockWatchingService) {
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
            LockWatchStarter lockWatchingService,
            LockWatchCachingConfig config) {
        Set<LockWatchReference> referencesFromSchema = schemas.stream()
                .map(Schema::getLockWatches)
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
        Set<TableReference> watchedTablesFromSchema = referencesFromSchema.stream()
                .map(schema -> schema.accept(LockWatchReferenceTableExtractor.INSTANCE))
                .flatMap(Optional::stream)
                .collect(Collectors.toSet());
        CacheMetrics metrics = CacheMetrics.create(metricsManager);
        LockWatchEventCache eventCache = LockWatchEventCacheImpl.create(metrics, config.maxEvents());
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
    void dumpState() {
        diagnosticLog.log(logger -> {
            logger.info(
                    "Dumping state from LockWatchManagerImpl",
                    UnsafeArg.of("referencesFromSchema", referencesFromSchema),
                    UnsafeArg.of("lockWatchReferences", new HashSet<>(lockWatchReferences)));
            lockWatchCache.dumpState();
        });
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
    public void onTransactionCommit(long startTs) {
        lockWatchCache.onTransactionCommit(startTs);
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
