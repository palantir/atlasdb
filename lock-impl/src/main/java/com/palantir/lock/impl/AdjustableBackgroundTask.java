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
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.io.Closeable;
import java.time.Duration;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public final class AdjustableBackgroundTask implements Closeable {

    /**
     * A minimum duration between invocations if the task is not actually enabled.
     * This is to prevent having a blocking thread in the background that does nothing.
     */
    @VisibleForTesting
    static final Duration MINIMUM_INTERVAL_IF_NOT_RUNNING = Duration.ofSeconds(1);

    private static final SafeLogger log = SafeLoggerFactory.get(AdjustableBackgroundTask.class);

    private final ScheduledExecutorService scheduledExecutor;
    private final Supplier<Boolean> shouldRunSupplier;
    private final Supplier<Duration> intervalSupplier;
    private final Runnable task;

    public static AdjustableBackgroundTask create(
            Supplier<Boolean> shouldRunSupplier, Supplier<Duration> delaySupplier, Runnable task) {
        return new AdjustableBackgroundTask(
                shouldRunSupplier, delaySupplier, task, PTExecutors.newSingleThreadScheduledExecutor());
    }

    @VisibleForTesting
    AdjustableBackgroundTask(
            Supplier<Boolean> shouldRunSupplier,
            Supplier<Duration> intervalSupplier,
            Runnable task,
            ScheduledExecutorService scheduledExecutor) {
        this.shouldRunSupplier = shouldRunSupplier;
        this.intervalSupplier = intervalSupplier;
        this.task = task;
        this.scheduledExecutor = scheduledExecutor;
        run();
    }

    private void run() {
        boolean shouldRun = shouldRunSupplier.get();
        if (shouldRun) {
            try {
                task.run();
            } catch (Exception e) {
                if (Thread.interrupted()) {
                    log.error("Task was interrupted. Stopping background execution.", e);
                    Thread.currentThread().interrupt();
                    return;
                }
                log.error("Exception occurred during task", e);
            } catch (Error e) {
                log.error("Error occurred during task. Stopping background execution.", e);
                return;
            }
        }
        try {
            long intervalMillis = intervalSupplier.get().toMillis();
            scheduledExecutor.schedule(
                    this::run,
                    shouldRun ? intervalMillis : Math.max(MINIMUM_INTERVAL_IF_NOT_RUNNING.toMillis(), intervalMillis),
                    TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            log.warn(
                    "Scheduler rejected execution",
                    SafeArg.of("schedulerIsShutdown", scheduledExecutor.isShutdown()),
                    e);
        }
    }

    @Override
    public void close() {
        scheduledExecutor.shutdown();
    }
}
