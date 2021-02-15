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

package com.palantir.atlasdb.sweep.queue;

import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;
import java.io.Closeable;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

public class ScalingSweepTaskScheduler implements Closeable {
    static final long INITIAL_DELAY = 1_000L;

    private final ScheduledExecutorService executorService;
    private final SweepDelay delay;
    private final Callable<SweepIterationResult> singleIteration;
    private final BooleanSupplier scalingEnabled;

    ScalingSweepTaskScheduler(
            ScheduledExecutorService executorService,
            SweepDelay delay,
            Callable<SweepIterationResult> singleIteration,
            BooleanSupplier scalingEnabled) {
        this.executorService = executorService;
        this.delay = delay;
        this.singleIteration = singleIteration;
        this.scalingEnabled = scalingEnabled;
    }

    /**
     * Creates a scheduler for targeted sweep background tasks that uses sweepPause to dynamically change pauses between
     * iterations.
     */
    public static ScalingSweepTaskScheduler createStarted(
            SweepDelay delay, int threads, Callable<SweepIterationResult> task, BooleanSupplier scalingEnabled) {
        ScheduledExecutorService executorService =
                PTExecutors.newScheduledThreadPoolExecutor(threads, new NamedThreadFactory("Targeted Sweep", true));

        ScalingSweepTaskScheduler scheduler =
                new ScalingSweepTaskScheduler(executorService, delay, task, scalingEnabled);
        scheduler.start(threads);
        return scheduler;
    }

    void start(int initialThreads) {
        for (int i = 0; i < initialThreads; i++) {
            scheduleAfterDelay(INITIAL_DELAY);
        }
    }

    private void scheduleAfterDelay(long pause) {
        executorService.schedule(() -> retryingTask(singleIteration), pause, TimeUnit.MILLISECONDS);
    }

    private void retryingTask(Callable<SweepIterationResult> task) {
        try {
            SweepIterationResult sweepResult = task.call();
            long pause = scalingEnabled.getAsBoolean() ? delay.getNextPause(sweepResult) : delay.getInitialPause();
            scheduleAfterDelay(pause);
        } catch (Exception e) {
            scheduleAfterDelay(delay.getMaxPause());
        }
    }

    @Override
    public void close() {
        executorService.shutdown();
    }
}
