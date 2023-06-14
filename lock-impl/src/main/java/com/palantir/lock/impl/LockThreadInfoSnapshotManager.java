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
import com.google.common.collect.ImmutableMap;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.lock.DebugThreadInfoConfiguration;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockClientAndThread;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.impl.LockServiceImpl.HeldLocks;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.refreshable.Disposable;
import com.palantir.refreshable.Refreshable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class LockThreadInfoSnapshotManager implements AutoCloseable {
    private Refreshable<DebugThreadInfoConfiguration> threadInfoConfiguration;

    private Supplier<ConcurrentMap<HeldLocksToken, HeldLocks<HeldLocksToken>>> tokenMapSupplier;

    private volatile Map<LockDescriptor, LockClientAndThread> lastKnownThreadInfoSnapshot = ImmutableMap.of();

    private ScheduledExecutorService scheduledExecutorService = PTExecutors.newSingleThreadScheduledExecutor();

    private Disposable disposable;

    private boolean isRunning = false;

    public LockThreadInfoSnapshotManager(
            Refreshable<DebugThreadInfoConfiguration> threadInfoConfiguration,
            Supplier<ConcurrentMap<HeldLocksToken, HeldLocks<HeldLocksToken>>> mapSupplier) {
        this.threadInfoConfiguration = threadInfoConfiguration;
        this.tokenMapSupplier = mapSupplier;
    }

    public void start() {
        scheduleRun();
        disposable = threadInfoConfiguration.subscribe(newThreadInfoConfiguration -> {
            synchronized (this) {
                if (!isRunning) {
                    scheduleRun();
                }
            }
        });
    }

    private void run() {
        takeSnapshot();

        // schedule next snapshot so this can be enabled/disabled at runtime
        scheduleRun();
    }

    @VisibleForTesting
    synchronized boolean isRunning() {
        return isRunning;
    }

    private synchronized void scheduleRun() {
        if (threadInfoConfiguration.current().recordThreadInfo()) {
            if (!isRunning) {
                isRunning = true;
            }
            scheduledExecutorService.schedule(
                    this::run,
                    threadInfoConfiguration.current().threadInfoSnapshotIntervalMillis(),
                    TimeUnit.MILLISECONDS);
        } else {
            isRunning = false;
        }
    }

    @VisibleForTesting
    Map<LockDescriptor, LockClientAndThread> getLastKnownThreadInfoSnapshot() {
        return lastKnownThreadInfoSnapshot;
    }

    /**
     * Returns a consistent snapshot of thread information restricted to the given lock descriptors wrapped in an
     * unsafe logging argument.
     */
    public UnsafeArg<Optional<Map<LockDescriptor, LockClientAndThread>>> getRestrictedSnapshotAsOptionalLogArg(
            Set<LockDescriptor> lockDescriptors) {
        String argName = "presumedClientThreadHoldersIfEnabled";
        if (!threadInfoConfiguration.current().recordThreadInfo()) {
            return UnsafeArg.of(argName, Optional.empty());
        }
        Map<LockDescriptor, LockClientAndThread> latestSnapshot = lastKnownThreadInfoSnapshot;
        Map<LockDescriptor, LockClientAndThread> restrictedSnapshot = new HashMap<>();
        for (LockDescriptor lock : lockDescriptors) {
            restrictedSnapshot.put(lock, latestSnapshot.get(lock));
        }
        return UnsafeArg.of(argName, Optional.of(restrictedSnapshot));
    }

    @VisibleForTesting
    void takeSnapshot() {
        lastKnownThreadInfoSnapshot = tokenMapSupplier.get().keySet().stream()
                .flatMap(token -> {
                    LockClientAndThread clientThread = LockClientAndThread.of(
                            Objects.requireNonNullElse(token.getClient(), LockClientAndThread.UNKNOWN.client()),
                            Objects.requireNonNullElse(
                                    token.getRequestingThread(), LockClientAndThread.UNKNOWN.thread()));
                    return token.getLockDescriptors().stream()
                            .map(lockDescriptor -> Map.entry(lockDescriptor, clientThread));
                })
                // Although a lock can be held by multiple clients/threads, we only remember one to save space
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue, (existing, replacement) -> existing));
    }

    @Override
    public void close() {
        scheduledExecutorService.shutdown();
        isRunning = true;
        disposable.dispose();
    }
}
