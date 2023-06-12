/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.lock.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.lock.DebugThreadInfoConfiguration;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.ThreadAwareLockClient;
import com.palantir.lock.impl.LockServiceImpl.HeldLocks;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class LockThreadInfoSnapshotManager {

    private DebugThreadInfoConfiguration threadInfoConfiguration;

    private Supplier<ConcurrentMap<HeldLocksToken, HeldLocks<HeldLocksToken>>> tokenMapSupplier;

    private Map<LockDescriptor, ThreadAwareLockClient> lastKnownThreadInfoSnapshot;

    private ScheduledExecutorService scheduledExecutorService = PTExecutors.newSingleThreadScheduledExecutor();

    public LockThreadInfoSnapshotManager(
            DebugThreadInfoConfiguration threadInfoConfiguration,
            Supplier<ConcurrentMap<HeldLocksToken, HeldLocks<HeldLocksToken>>> mapSupplier) {
        this.threadInfoConfiguration = threadInfoConfiguration;
        this.tokenMapSupplier = mapSupplier;
        this.lastKnownThreadInfoSnapshot = ImmutableMap.of();

        scheduleSnapshotting();
    }

    private void scheduleSnapshotting() {
        if (threadInfoConfiguration.recordThreadInfo()) {
            scheduledExecutorService.schedule(
                    this::takeSnapshot,
                    threadInfoConfiguration.threadInfoSnapshotIntervalMillis(),
                    TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Returns a consistent snapshot of tread information restricted to the given lock descriptors
     */
    public Map<LockDescriptor, ThreadAwareLockClient> getLastKnownThreadInfoSnapshot(
            Set<LockDescriptor> lockDescriptors) {
        final Map<LockDescriptor, ThreadAwareLockClient> currentSnapshot = this.lastKnownThreadInfoSnapshot;
        return lockDescriptors.stream()
                .filter(currentSnapshot::containsKey)
                .collect(Collectors.toMap(lock -> lock, currentSnapshot::get));
    }

    @VisibleForTesting
    void takeSnapshot() {
        this.lastKnownThreadInfoSnapshot = tokenMapSupplier.get().keySet().stream()
                .flatMap(token -> {
                    ThreadAwareLockClient threadAwareLockClient =
                            token.getClient() == null || Strings.isNullOrEmpty(token.getRequestingThread())
                                    ? ThreadAwareLockClient.UNKNOWN
                                    : ThreadAwareLockClient.of(token.getClient(), token.getRequestingThread());
                    return token.getLockDescriptors().stream()
                            .map(lockDescriptor -> Map.entry(lockDescriptor, threadAwareLockClient));
                })
                // Although a lock can be held by multiple clients/threads, we only remember one to save space
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue, (existing, replacement) -> existing));

        // schedule next snapshot so this can be enabled/disabled at runtime
        scheduleSnapshotting();
    }
}
