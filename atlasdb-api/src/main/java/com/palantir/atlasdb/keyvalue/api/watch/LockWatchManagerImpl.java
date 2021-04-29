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

package com.palantir.atlasdb.keyvalue.api.watch;

import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.timelock.api.LockWatchRequest;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.lock.client.NamespacedConjureLockWatchingService;
import com.palantir.lock.watch.CommitUpdate;
import com.palantir.lock.watch.LockWatchCache;
import com.palantir.lock.watch.LockWatchReferences;
import com.palantir.lock.watch.LockWatchVersion;
import com.palantir.lock.watch.TransactionsLockWatchUpdate;
import com.palantir.logsafe.UnsafeArg;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class LockWatchManagerImpl extends LockWatchManagerInternal implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(LockWatchManagerImpl.class);

    private final Set<LockWatchReferences.LockWatchReference> referencesFromSchema;
    private final Set<LockWatchReferences.LockWatchReference> lockWatchReferences = ConcurrentHashMap.newKeySet();
    private final LockWatchCache lockWatchCache;
    private final NamespacedConjureLockWatchingService lockWatchingService;
    private final ScheduledExecutorService executorService = PTExecutors.newSingleThreadScheduledExecutor();
    private final ScheduledFuture<?> refreshTask;

    public LockWatchManagerImpl(
            Set<Schema> schemas,
            LockWatchCache lockWatchCache,
            NamespacedConjureLockWatchingService lockWatchingService) {
        this.referencesFromSchema = schemas.stream()
                .map(Schema::getLockWatches)
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
        this.lockWatchCache = lockWatchCache;
        this.lockWatchingService = lockWatchingService;
        lockWatchReferences.addAll(referencesFromSchema);
        refreshTask = executorService.scheduleWithFixedDelay(this::registerWatchesWithTimelock, 0, 5, TimeUnit.SECONDS);
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

    private void registerWatchesWithTimelock() {
        if (lockWatchReferences.isEmpty()) {
            return;
        }

        try {
            lockWatchingService.startWatching(LockWatchRequest.of(lockWatchReferences));
        } catch (Throwable e) {
            log.info("Failed to register lockwatches", UnsafeArg.of("lockwatches", lockWatchReferences));
        }
    }
}
