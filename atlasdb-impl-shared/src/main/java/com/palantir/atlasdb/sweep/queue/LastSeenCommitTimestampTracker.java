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
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class LastSeenCommitTimestampTracker implements AutoCloseable {
    private static final SafeLogger log = SafeLoggerFactory.get(LastSeenCommitTimestampTracker.class);

    private final ShardProgress shardProgress;
    private final ScheduledExecutorService executor;

    // Todo(snanda)
    private volatile OptionalLong oldestTimestamp = OptionalLong.empty();

    @VisibleForTesting
    public LastSeenCommitTimestampTracker(KeyValueService kvs, ScheduledExecutorService executor) {
        this.shardProgress = new ShardProgress(kvs);
        this.executor = executor;
    }

    public OptionalLong getLastSeenCommitTimestamp() {
        return shardProgress.getLastSeenCommitTimestamp().map(OptionalLong::of).orElseGet(OptionalLong::empty);
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
                // todo(snanda)
            }
            persistLastSeenCommitTimestamp();
            executor.shutdownNow();
            return true;
        } catch (Exception e) {
            log.info(
                    "Failed to persist last seen commit timestamp by sweep",
                    SafeArg.of("lastSeenCommitTs", oldestTimestamp),
                    e);
            return false;
        }
    }

    private void persistLastSeenCommitTimestamp() {
        Optional<Long> persisted = shardProgress.getLastSeenCommitTimestamp();
        if (persisted.map(actual -> actual > oldestTimestamp.getAsLong()).orElse(true)) {
            shardProgress.tryUpdateLastSeenCommitTimestamp(persisted, oldestTimestamp.getAsLong());
        }
    }

    @Override
    public void close() throws Exception {
        executor.shutdownNow();
    }
}
