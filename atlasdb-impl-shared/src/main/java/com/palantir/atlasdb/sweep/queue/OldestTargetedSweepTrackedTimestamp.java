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

package com.palantir.atlasdb.sweep.queue;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.sweep.queue.config.TargetedSweepInstallConfig;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.timestamp.TimestampService;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * WARNING: The correctness of using this information depends on
 * {@link TargetedSweepInstallConfig#enableSweepQueueWrites()} being true on every service node since the moment
 * this is first scheduled.
 */
public class OldestTargetedSweepTrackedTimestamp implements AutoCloseable {
    private static final SafeLogger log = SafeLoggerFactory.get(OldestTargetedSweepTrackedTimestamp.class);

    private final ShardProgress shardProgress;
    private final LongSupplier timestampService;
    private final ScheduledExecutorService executor;

    private volatile OptionalLong oldestTimestamp = OptionalLong.empty();

    @VisibleForTesting
    OldestTargetedSweepTrackedTimestamp(
            KeyValueService kvs, LongSupplier timestampService, ScheduledExecutorService executor) {
        this.shardProgress = new ShardProgress(kvs);
        this.timestampService = timestampService;
        this.executor = executor;
    }

    public static OldestTargetedSweepTrackedTimestamp createStarted(
            KeyValueService kvs, TimestampService timestampService, ScheduledExecutorService executor) {
        OldestTargetedSweepTrackedTimestamp timestampTracker =
                new OldestTargetedSweepTrackedTimestamp(kvs, timestampService::getFreshTimestamp, executor);
        timestampTracker.start();
        return timestampTracker;
    }

    public OptionalLong getOldestTimestamp() {
        return shardProgress.getOldestSeenTimestamp().map(OptionalLong::of).orElseGet(OptionalLong::empty);
    }

    @VisibleForTesting
    void start() {
        executor.schedule(this::runOneIteration, 0, TimeUnit.SECONDS);
    }

    void runOneIteration() {
        if (!tryToPersist()) {
            executor.schedule(this::runOneIteration, 1, TimeUnit.SECONDS);
        }
    }

    private boolean tryToPersist() {
        try {
            if (oldestTimestamp.isEmpty()) {
                oldestTimestamp = OptionalLong.of(timestampService.getAsLong());
            }
            persist();
            executor.shutdownNow();
            return true;
        } catch (Exception e) {
            log.info("Failed to persist timestamp", SafeArg.of("oldestSeenTs", oldestTimestamp), e);
            return false;
        }
    }

    private void persist() {
        Optional<Long> persisted = shardProgress.getOldestSeenTimestamp();
        if (persisted.map(actual -> actual > oldestTimestamp.getAsLong()).orElse(true)) {
            shardProgress.tryUpdateOldestSeenTimestamp(persisted, oldestTimestamp.getAsLong());
        }
    }

    @Override
    public void close() throws Exception {
        executor.shutdownNow();
    }
}
