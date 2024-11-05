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

import com.palantir.atlasdb.sweep.asts.locks.Lockable;
import com.palantir.atlasdb.sweep.asts.locks.Lockable.LockedItem;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.refreshable.Refreshable;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

// This feels like it should already exist.
public final class DynamicTaskScheduler {
    private static final SafeLogger log = SafeLoggerFactory.get(DynamicTaskScheduler.class);
    private final ScheduledExecutorService scheduledExecutorService;
    private final Refreshable<Duration> automaticSweepRefreshDelay;
    private final Runnable task;
    private final AtomicBoolean isStarted = new AtomicBoolean(false);

    // TODO(mdaudali): Use MDC
    private final String safeLoggableTaskName;

    private DynamicTaskScheduler(
            ScheduledExecutorService scheduledExecutorService,
            Refreshable<Duration> automaticSweepRefreshDelay,
            Runnable task,
            String safeLoggableTaskName) {
        this.scheduledExecutorService = scheduledExecutorService;
        this.automaticSweepRefreshDelay = automaticSweepRefreshDelay;
        this.task = task;
        this.safeLoggableTaskName = safeLoggableTaskName;
    }

    public static DynamicTaskScheduler create(
            ScheduledExecutorService scheduledExecutorService,
            Refreshable<Duration> automaticSweepRefreshDelay,
            Runnable task,
            String safeLoggableTaskName) {
        return new DynamicTaskScheduler(
                scheduledExecutorService, automaticSweepRefreshDelay, task, safeLoggableTaskName);
    }

    public static DynamicTaskScheduler createForExclusiveTask(
            ScheduledExecutorService scheduledExecutorService,
            Refreshable<Duration> automaticSweepRefreshDelay,
            Lockable<ExclusiveTask> exclusiveTask,
            String safeLoggableTaskName) {
        Runnable runnable = () -> exclusiveTask
                .tryLock(_ignored -> {})
                .ifPresentOrElse(
                        item -> {
                            try (LockedItem<ExclusiveTask> task = item) {
                                task.getItem().task().run();
                            }
                        },
                        () -> log.warn(
                                "Failed to acquire lock for task: {}", SafeArg.of("task", safeLoggableTaskName)));
        return new DynamicTaskScheduler(
                scheduledExecutorService, automaticSweepRefreshDelay, runnable, safeLoggableTaskName);
    }

    public void start() {
        if (isStarted.compareAndSet(false, true)) {
            scheduleNextIteration(automaticSweepRefreshDelay.get());
        } else {
            log.warn("Attempted to start an already started task: {}", SafeArg.of("task", safeLoggableTaskName));
        }
    }

    private void runOneIteration() {
        Duration delay = automaticSweepRefreshDelay.get();
        try {
            if (log.isDebugEnabled()) {
                log.debug("Running task: {}", SafeArg.of("task", safeLoggableTaskName));
            }
            task.run();
        } catch (Exception e) {
            log.warn(
                    "Failed to run task {}. Will retry after delay {}",
                    SafeArg.of("task", safeLoggableTaskName),
                    SafeArg.of("delay", delay),
                    e);
        }
        scheduleNextIteration(delay);
    }

    private void scheduleNextIteration(Duration delay) {
        if (log.isDebugEnabled()) {
            log.debug(
                    "Scheduling next iteration for {} with delay {}",
                    SafeArg.of("task", safeLoggableTaskName),
                    SafeArg.of("delay", delay));
        }
        scheduledExecutorService.schedule(this::runOneIteration, delay.toMillis(), TimeUnit.MILLISECONDS);
    }
}
