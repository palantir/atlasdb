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
import com.palantir.lock.SimpleTimeDuration;
import com.palantir.lock.TimeDuration;
import java.io.Closeable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public final class AdjustableBackgroundTask implements Closeable {
    /** A minimum duration between invocations if the task is not actually enabled.
     * This is to prevent having a blocking thread in the background that does nothing.
     */
    @VisibleForTesting
    static final TimeDuration MINIMUM_DELAY_IF_NOT_RUNNING = SimpleTimeDuration.of(1, TimeUnit.SECONDS);

    private final ScheduledExecutorService scheduledExecutor;
    private final Supplier<Boolean> shouldRunSupplier;
    private final Supplier<TimeDuration> intervalSupplier;
    private final Runnable task;

    public static AdjustableBackgroundTask create(
            Supplier<Boolean> shouldRunSupplier, Supplier<TimeDuration> delaySupplier, Runnable task) {
        return new AdjustableBackgroundTask(
                shouldRunSupplier, delaySupplier, task, PTExecutors.newSingleThreadScheduledExecutor());
    }

    @VisibleForTesting
    AdjustableBackgroundTask(
            Supplier<Boolean> shouldRunSupplier,
            Supplier<TimeDuration> intervalSupplier,
            Runnable task,
            ScheduledExecutorService scheduledExecutor) {
        this.shouldRunSupplier = shouldRunSupplier;
        this.intervalSupplier = intervalSupplier;
        this.task = task;
        this.scheduledExecutor = scheduledExecutor;
        run();
    }

    private synchronized void run() {
        if (shouldRunSupplier.get()) {
            task.run();
        }
        if (!scheduledExecutor.isShutdown()) {
            scheduledExecutor.schedule(
                    this::run,
                    Math.max(
                            MINIMUM_DELAY_IF_NOT_RUNNING.toMillis(),
                            intervalSupplier.get().toMillis()),
                    TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public synchronized void close() {
        scheduledExecutor.shutdown();
    }
}
