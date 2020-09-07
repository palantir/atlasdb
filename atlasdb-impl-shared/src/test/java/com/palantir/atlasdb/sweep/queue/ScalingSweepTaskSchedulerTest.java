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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static com.palantir.atlasdb.sweep.queue.ScalingSweepTaskScheduler.INITIAL_DELAY;
import static com.palantir.logsafe.testing.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.Before;
import org.junit.Test;

import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.common.concurrent.PTExecutors;

public class ScalingSweepTaskSchedulerTest {
    private static final SweepIterationResult SUCCESS_HUGE = SweepIterationResults
            .success(SweepQueueUtils.SWEEP_BATCH_SIZE);
    private static final SweepIterationResult SUCCESS_LARGE = SweepIterationResults
            .success(ScalingSweepTaskScheduler.BATCH_CELLS_HIGH_THRESHOLD);
    private static final SweepIterationResult SUCCESS_MEDIUM = SweepIterationResults
            .success(ScalingSweepTaskScheduler.BATCH_CELLS_LOW_THRESHOLD + 1);
    private static final SweepIterationResult SUCCESS_SMALL = SweepIterationResults
            .success(ScalingSweepTaskScheduler.BATCH_CELLS_LOW_THRESHOLD);
    private static final SweepIterationResult SUCCESS_TINY = SweepIterationResults
            .success(SweepDelay.BATCH_CELLS_LOW_THRESHOLD);
    private static final long DELAY = 1L;
    private static final long INITIAL_PAUSE = 5L;

    private final DeterministicScheduler deterministicScheduler = new DeterministicScheduler();
    private final SweepDelay delay = mock(SweepDelay.class);
    private final Callable<SweepIterationResult> sweepIteration = mock(Callable.class);
    private final AtomicBoolean schedulerEnabled = new AtomicBoolean(true);
    private final ScalingSweepTaskScheduler scheduler = createScheduler(Duration.ZERO);

    @Before
    public void setup() {
        when(delay.getInitialPause()).thenReturn(INITIAL_PAUSE);
        when(delay.getNextPause(any(SweepIterationResult.class))).thenReturn(DELAY);
    }

    @Test
    public void whenExpectedNumberOfEntriesIsSweptKeepReschedulingAfterDelay() throws Exception {
        when(sweepIteration.call()).thenReturn(SUCCESS_MEDIUM);
        scheduler.start(2);

        deterministicScheduler.tick(INITIAL_DELAY + 8 * DELAY, TimeUnit.MILLISECONDS);
        verify(sweepIteration, times((1 + 8) * 2)).call();
    }

    @Test
    public void whenScalingDisabledUsesInitialPause() throws Exception {
        when(sweepIteration.call()).thenReturn(SUCCESS_MEDIUM);
        schedulerEnabled.set(false);
        scheduler.start(10);

        deterministicScheduler.tick(INITIAL_DELAY + 7 * INITIAL_PAUSE, TimeUnit.MILLISECONDS);
        verify(sweepIteration, times(8 * 10)).call();
    }

    @Test
    public void whenManyEntriesAreSweptNewTaskSpawns() throws Exception {
        when(sweepIteration.call()).thenReturn(
                SUCCESS_LARGE,
                SUCCESS_MEDIUM, SUCCESS_MEDIUM,
                SUCCESS_LARGE, SUCCESS_MEDIUM);
        scheduler.start(1);

        deterministicScheduler.tick(INITIAL_DELAY + 5 * DELAY, TimeUnit.MILLISECONDS);
        verify(sweepIteration, times(1 + 2 + 2 + 3 + 3 + 3)).call();
    }

    @Test
    public void coolDownProtectsAgainstSpawningNewTasks() throws Exception {
        ScalingSweepTaskScheduler schedulerWithCoolDown = createScheduler(Duration.ofDays(1));
        when(sweepIteration.call()).thenReturn(SUCCESS_LARGE);
        schedulerWithCoolDown.start(1);

        deterministicScheduler.tick(INITIAL_DELAY + 5 * DELAY, TimeUnit.MILLISECONDS);
        verify(sweepIteration, times(1 + 5)).call();
    }

    @Test
    public void whenFewEntriesAreSweptTasksAreReduced() throws Exception {
        when(sweepIteration.call()).thenReturn(SUCCESS_SMALL, SUCCESS_MEDIUM);
        scheduler.start(10);

        deterministicScheduler.tick(INITIAL_DELAY + 5 * DELAY, TimeUnit.MILLISECONDS);
        verify(sweepIteration, times(10 + 5 * 9)).call();
    }

    @Test
    public void whenFewEntriesDoNotReduceToZeroTasks() throws Exception {
        when(sweepIteration.call()).thenReturn(SUCCESS_SMALL);
        scheduler.start(1);
        deterministicScheduler.tick(INITIAL_DELAY + 5 * DELAY, TimeUnit.MILLISECONDS);
        verify(sweepIteration, times(1 + 5)).call();
    }

    @Test
    public void coolDownProtectsAgainstReducingTasks() throws Exception {
        ScalingSweepTaskScheduler schedulerWithCoolDown = createScheduler(Duration.ofDays(1));
        when(sweepIteration.call()).thenReturn(SUCCESS_SMALL);

        schedulerWithCoolDown.start(2);

        deterministicScheduler.tick(INITIAL_DELAY + 5 * DELAY, TimeUnit.MILLISECONDS);
        verify(sweepIteration, times(2 + 5 * 2)).call();
    }

    @Test
    public void attemptToIncreaseNumberOfThreadsPreventsReduction() throws Exception {
        Duration coolDown = Duration.ofSeconds(1);
        ScalingSweepTaskScheduler schedulerWithCoolDown = createScheduler(coolDown);
        when(sweepIteration.call()).thenReturn(
                SUCCESS_LARGE,
                SUCCESS_LARGE,
                SUCCESS_SMALL);
        schedulerWithCoolDown.start(2);

        Uninterruptibles.sleepUninterruptibly(coolDown.toMillis() / 2 + 1, TimeUnit.MILLISECONDS);
        deterministicScheduler.tick(INITIAL_DELAY, TimeUnit.MILLISECONDS);

        Uninterruptibles.sleepUninterruptibly(coolDown.toMillis() / 2 + 1, TimeUnit.MILLISECONDS);
        // these will not reduce the number of threads due to the attempt to increase in previous iterations
        deterministicScheduler.tick(2 * DELAY, TimeUnit.MILLISECONDS);

        Uninterruptibles.sleepUninterruptibly(coolDown.toMillis() / 2 + 1, TimeUnit.MILLISECONDS);
        // the first iteration that gets executed will not reschedule
        deterministicScheduler.tick(2 * DELAY, TimeUnit.MILLISECONDS);

        verify(sweepIteration, times(2 + 2 * 2 + 2 + 1)).call();
    }

    @Test
    public void whenUnableToAcquireShardOnLastTaskRescheduleAfterMaxPause() throws Exception {
        ScalingSweepTaskScheduler schedulerWithRealDelay = createScheduler(new SweepDelay(1L), Duration.ZERO);
        when(sweepIteration.call()).thenReturn(SweepIterationResults.unableToAcquireShard(), SUCCESS_MEDIUM);

        schedulerWithRealDelay.start(1);
        deterministicScheduler.tick(INITIAL_DELAY + SweepDelay.DEFAULT_MAX_PAUSE_MILLIS + 2, TimeUnit.MILLISECONDS);
        verify(sweepIteration, times(1 + 3)).call();
    }

    @Test
    public void whenInsufficientConsistencyRescheduleAfterBackoff() throws Exception {
        ScalingSweepTaskScheduler schedulerWithRealDelay = createScheduler(new SweepDelay(1L), Duration.ZERO);
        when(sweepIteration.call()).thenReturn(
                SweepIterationResults.insufficientConsistency(),
                SweepIterationResults.insufficientConsistency(),
                SUCCESS_MEDIUM);

        schedulerWithRealDelay.start(2);
        deterministicScheduler.tick(INITIAL_DELAY + SweepDelay.BACKOFF + 2, TimeUnit.MILLISECONDS);
        verify(sweepIteration, times((1 + 3) * 2)).call();
    }

    @Test
    public void whenDisabledOnLastTaskRescheduleAfterBackoff() throws Exception {
        ScalingSweepTaskScheduler schedulerWithRealDelay = createScheduler(new SweepDelay(1L), Duration.ZERO);
        when(sweepIteration.call()).thenReturn(SweepIterationResults.disabled(), SUCCESS_MEDIUM);

        schedulerWithRealDelay.start(1);
        deterministicScheduler.tick(INITIAL_DELAY + SweepDelay.BACKOFF + 2, TimeUnit.MILLISECONDS);
        verify(sweepIteration, times(1 + 3)).call();
    }

    @Test
    public void whenOtherErrorRescheduleAfterMaxPause() throws Exception {
        ScalingSweepTaskScheduler schedulerWithRealDelay = createScheduler(new SweepDelay(1L), Duration.ZERO);
        when(sweepIteration.call()).thenReturn(
                SweepIterationResults.otherError(),
                SweepIterationResults.otherError(),
                SUCCESS_MEDIUM);

        schedulerWithRealDelay.start(2);
        deterministicScheduler.tick(INITIAL_DELAY + SweepDelay.DEFAULT_MAX_PAUSE_MILLIS + 2, TimeUnit.MILLISECONDS);
        verify(sweepIteration, times((1 + 3) * 2)).call();
    }

    @Test
    public void whenUnableToAcquireShardReduceNumberOfTasksIgnoringCoolDown() throws Exception {
        ScalingSweepTaskScheduler schedulerWithCoolDown = createScheduler(Duration.ofDays(1));
        when(sweepIteration.call()).thenReturn(
                SUCCESS_SMALL,
                SUCCESS_SMALL,
                SUCCESS_SMALL,
                SweepIterationResults.unableToAcquireShard());

        schedulerWithCoolDown.start(3);
        deterministicScheduler.tick(INITIAL_DELAY + 2 * DELAY, TimeUnit.MILLISECONDS);
        verify(sweepIteration, times(3 + 3 + 1)).call();
    }

    @Test
    public void whenDisabledReduceNumberOfTasksIgnoringCoolDown() throws Exception {
        ScalingSweepTaskScheduler schedulerWithCoolDown = createScheduler(Duration.ofDays(1));
        when(sweepIteration.call()).thenReturn(
                SUCCESS_SMALL,
                SUCCESS_SMALL,
                SUCCESS_SMALL,
                SweepIterationResults.disabled());

        schedulerWithCoolDown.start(3);
        deterministicScheduler.tick(INITIAL_DELAY + 2 * DELAY, TimeUnit.MILLISECONDS);
        verify(sweepIteration, times(3 + 3 + 1)).call();
    }

    @Test
    public void whenVeryFewEntriesIncreasePause() throws Exception {
        ScalingSweepTaskScheduler schedulerWithRealDelay = createScheduler(new SweepDelay(100L), Duration.ofDays(1));
        when(sweepIteration.call()).thenReturn(SUCCESS_TINY);

        schedulerWithRealDelay.start(3);
        deterministicScheduler.tick(INITIAL_DELAY + 100L * 10, TimeUnit.MILLISECONDS);
        verify(sweepIteration, atMost(3 * 10 - 1)).call();
    }

    @Test
    public void whenVeryManyEntriesDecreasePause() throws Exception {
        ScalingSweepTaskScheduler schedulerWithRealDelay = createScheduler(new SweepDelay(100L), Duration.ofDays(1));
        when(sweepIteration.call()).thenReturn(SUCCESS_HUGE);

        schedulerWithRealDelay.start(3);
        deterministicScheduler.tick(INITIAL_DELAY + 100L * 9, TimeUnit.MILLISECONDS);
        verify(sweepIteration, atLeast(3 * 10 + 1)).call();
    }

    @Test
    public void stressTestThatNumberOfTasksStaysWithinBounds() {
        AtomicLong iterationCount = new AtomicLong(0L);
        AtomicReference<SweepIterationResult> result = new AtomicReference<>(SUCCESS_SMALL);
        Callable<SweepIterationResult> oneIteration = () -> {
            iterationCount.incrementAndGet();
            return result.get();
        };
        ScalingSweepTaskScheduler nondeterministicScheduler = new ScalingSweepTaskScheduler(
                PTExecutors.newScheduledThreadPool(1),
                delay,
                Duration.ZERO,
                oneIteration,
                schedulerEnabled::get);

        nondeterministicScheduler.start(10);
        Uninterruptibles.sleepUninterruptibly(INITIAL_DELAY, TimeUnit.MILLISECONDS);

        for (int i = 0; i < 10; i++) {
            result.set(SUCCESS_SMALL);
            Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);

            // we only have one task remaining
            result.set(SUCCESS_MEDIUM);
            iterationCount.set(0);
            Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
            assertThat(iterationCount.get()).isBetween(50L, 200L);

            result.set(SUCCESS_LARGE);
            Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);

            // we have 128 tasks
            result.set(SUCCESS_MEDIUM);
            iterationCount.set(0);
            Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
            assertThat(iterationCount.get()).isBetween(50 * 128L, 200 * 128L);
        }
    }

    private ScalingSweepTaskScheduler createScheduler(Duration coolDown) {
        return createScheduler(delay, coolDown);
    }

    private ScalingSweepTaskScheduler createScheduler(SweepDelay sweepDelay, Duration coolDown) {
        return new ScalingSweepTaskScheduler(
                deterministicScheduler,
                sweepDelay,
                coolDown,
                sweepIteration,
                schedulerEnabled::get);
    }
}
