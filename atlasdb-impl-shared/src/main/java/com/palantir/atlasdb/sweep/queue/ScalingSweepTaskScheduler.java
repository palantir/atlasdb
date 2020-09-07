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

import java.io.Closeable;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;

public class ScalingSweepTaskScheduler implements Closeable {
    private static final Duration COOL_DOWN = Duration.ofMinutes(5L);
    static final int BATCH_CELLS_LOW_THRESHOLD = 1_000;
    static final int BATCH_CELLS_HIGH_THRESHOLD = SweepQueueUtils.SWEEP_BATCH_SIZE * 2 / 3;
    static final long INITIAL_DELAY = 1000L;

    private final ScheduledExecutorService executorService;
    private final SweepDelay delay;
    private final Duration coolDown;
    private final Callable<SweepIterationResult> singleIteration;
    private final BooleanSupplier scalingEnabled;

    private int runningTasks = 0;
    private Instant lastModification = Instant.now();
    private Instant lastIncreaseAttempted = Instant.now();

    ScalingSweepTaskScheduler(
            ScheduledExecutorService executorService,
            SweepDelay delay,
            Duration coolDown,
            Callable<SweepIterationResult> singleIteration,
            BooleanSupplier scalingEnabled) {
        this.executorService = executorService;
        this.delay = delay;
        this.coolDown = coolDown;
        this.singleIteration = singleIteration;
        this.scalingEnabled = scalingEnabled;
    }

    /**
     * Creates a scheduler for targeted sweep background tasks that dynamically modifies the number of parallel tasks
     * based on results. The number of tasks is guaranteed to always be between 1 nad 128, and will only change by one
     * in any {@link #COOL_DOWN} period. Furthermore, if conflicting results are observed, increasing the number of tasks
     * is prioritised to make sure targeted sweep does not fall behind.
     *
     * If an iteration of the task is unable to acquire a shard to sweep or sweep is disabled, and there are multiple
     * running tasks, the number of tasks will be decreased regardless of {@link #COOL_DOWN} as this indicates the level
     * of parallelism is too high to achieve any benefit.
     */
    public static ScalingSweepTaskScheduler createStarted(
            SweepDelay delay,
            int initialThreads,
            Callable<SweepIterationResult> task,
            BooleanSupplier scalingEnabled) {
        ScheduledExecutorService  executorService = PTExecutors.newScheduledThreadPoolExecutor(1,
                new NamedThreadFactory("Targeted Sweep", true));

        ScalingSweepTaskScheduler scheduler = new ScalingSweepTaskScheduler(
                executorService, delay, COOL_DOWN, task, scalingEnabled);
        scheduler.start(initialThreads);
        return scheduler;
    }

    void start(int initialThreads) {
        for (int i = 0; i < initialThreads; i++) {
            increaseNumberOfTasks(INITIAL_DELAY);
        }
    }

    private synchronized void maybeIncreaseNumberOfTasks(long pause) {
        lastIncreaseAttempted = Instant.now();
        if (cooldownPassed(lastModification)) {
            increaseNumberOfTasks(pause);
        }
        scheduleAfterDelay(pause);
    }

    private synchronized void maybeDecreaseNumberOfTasks(long pause) {
        if (cooldownPassed(lastModification) && cooldownPassed(lastIncreaseAttempted)) {
            decreaseNumberOfTasksOrRescheduleIfLast(pause);
        } else {
            scheduleAfterDelay(pause);
        }
    }

    private synchronized void increaseNumberOfTasks(long pause) {
        if (runningTasks < 128) {
            runningTasks++;
            lastModification = Instant.now();
            scheduleAfterDelay(pause);
        }
    }

    private synchronized void decreaseNumberOfTasksOrRescheduleIfLast(long pause) {
        if (runningTasks == 1) {
            scheduleAfterDelay(pause);
        } else {
            decreaseNumberOfTasks();
        }
    }

    private synchronized void decreaseNumberOfTasks() {
        runningTasks--;
        lastModification = Instant.now();
    }

    private void scheduleAfterDelay(long pause) {
        executorService.schedule(() -> retryingTask(singleIteration), pause, TimeUnit.MILLISECONDS);
    }

    private void retryingTask(Callable<SweepIterationResult> task) {
        try {
            SweepIterationResult sweepResult = task.call();
            if (!scalingEnabled.getAsBoolean()) {
                scheduleAfterDelay(delay.getInitialPause());
            }
            long pause = delay.getNextPause(sweepResult);
            SweepIterationResults.caseOf(sweepResult)
                    .success(numThreads -> determineAction(numThreads, pause))
                    .unableToAcquireShard(wrap(() -> decreaseNumberOfTasksOrRescheduleIfLast(pause)))
                    .insufficientConsistency(wrap(() -> scheduleAfterDelay(pause)))
                    .otherError(wrap(() -> scheduleAfterDelay(pause)))
                    .disabled(wrap(() -> decreaseNumberOfTasksOrRescheduleIfLast(pause)));
        } catch (Exception e) {
            scheduleAfterDelay(delay.getMaxPause());
        }
    }

    private Void determineAction(long numThreads, long pause) {
        if (numThreads <= BATCH_CELLS_LOW_THRESHOLD) {
            maybeDecreaseNumberOfTasks(pause);
        } else if (numThreads >= BATCH_CELLS_HIGH_THRESHOLD) {
            maybeIncreaseNumberOfTasks(pause);
        } else {
            scheduleAfterDelay(pause);
        }
        return null;
    }

    private boolean cooldownPassed(Instant lastOccurrence) {
        return Duration.between(lastOccurrence, Instant.now()).compareTo(coolDown) >= 0;
    }

    private static <R> Supplier<R> wrap(Runnable runnable) {
        return () -> {
            runnable.run();
            return null;
        };
    }

    @Override
    public void close() {
        executorService.shutdown();
    }
}
