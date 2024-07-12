/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.sweep.asts;

import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.refreshable.Refreshable;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

// This feels like it should already exist.
public final class AutoUpdateBucketTask {
    private static final SafeLogger log = SafeLoggerFactory.get(AutoUpdateBucketTask.class);
    private final ScheduledExecutorService scheduledExecutorService;
    private final Refreshable<Duration> automaticSweepRefreshDelay;
    private final Runnable task;

    private AutoUpdateBucketTask(
            ScheduledExecutorService scheduledExecutorService,
            Refreshable<Duration> automaticSweepRefreshDelay,
            Runnable task) {
        this.scheduledExecutorService = scheduledExecutorService;
        this.automaticSweepRefreshDelay = automaticSweepRefreshDelay;
        this.task = task;
        scheduleNextIteration(automaticSweepRefreshDelay.get()); // Probably should move to a start function
    }

    public static AutoUpdateBucketTask create(
            ScheduledExecutorService scheduledExecutorService,
            Refreshable<Duration> automaticSweepRefreshDelay,
            Runnable task) {
        return new AutoUpdateBucketTask(scheduledExecutorService, automaticSweepRefreshDelay, task);
    }

    private void runOneIteration() {
        Duration delay = automaticSweepRefreshDelay.get();
        try {
            task.run();
        } catch (Exception e) {
            log.warn("Failed to run task. Will retry in the next interval", SafeArg.of("delay", delay), e);
        }
        scheduleNextIteration(delay);
    }

    private void scheduleNextIteration(Duration delay) {
        scheduledExecutorService.schedule(this::runOneIteration, delay.toMillis(), TimeUnit.MILLISECONDS);
    }
}
