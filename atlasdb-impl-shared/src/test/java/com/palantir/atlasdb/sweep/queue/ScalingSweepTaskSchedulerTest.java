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
import static com.palantir.atlasdb.sweep.queue.ScalingSweepTaskScheduler.MAX_PARALLELISM;
import static com.palantir.logsafe.testing.Assertions.assertThat;

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

    private final AtomicLong clockMillis = new AtomicLong(0);
    private final DeterministicScheduler deterministicScheduler = new DeterministicScheduler();
    private final SweepDelay delay = mock(SweepDelay.class);
    private final Callable<SweepIterationResult> sweepIteration = mock(Callable.class);
    private final AtomicBoolean schedulerEnabled = new AtomicBoolean(true);
    private final ScalingSweepTaskScheduler scheduler = createScheduler(delay);
    private final ScalingSweepTaskScheduler schedulerWithDelay = createScheduler(new SweepDelay(DELAY));

    private boolean firstIteration = true;

    @Before
    public void setup() {
        when(delay.getInitialPause()).thenReturn(INITIAL_PAUSE);
        when(delay.getNextPause(any(SweepIterationResult.class))).thenReturn(DELAY);
    }

    @Test
    public void withExpectedEntriesKeepReschedulingAfterDelay() throws Exception {
        when(sweepIteration.call()).thenReturn(SUCCESS_MEDIUM);
        scheduler.start(2);

        runSweepIterations(5);
        tickClock();
        runSweepIterations(5);
        verify(sweepIteration, times(10 * 2)).call();
    }

    @Test
    public void whenScalingDisabledUseInitialPause() throws Exception {
        when(sweepIteration.call()).thenReturn(SUCCESS_MEDIUM);
        scheduler.start(2);

        runSweepIterations(5);

        schedulerEnabled.set(false);
        runSweepIterations(5, INITIAL_PAUSE);
        verify(sweepIteration, times(20)).call();
    }

    @Test
    public void noTasksAreSpawnedBeforeCoolDownPasses() throws Exception {
        when(sweepIteration.call()).thenReturn(SUCCESS_LARGE);

        scheduler.start(1);
        runSweepIterations(10);
        verify(sweepIteration, times(10)).call();
    }

    @Test
    public void withManyEntriesSpawnOneTaskEachTimeCoolDownPasses() throws Exception {
        when(sweepIteration.call()).thenReturn(SUCCESS_LARGE);
        scheduler.start(1);

        tickClock();
        runSweepIterations(1 + 4);

        tickClockForHalfCoolDown();
        runSweepIterations(5);

        tickClockForHalfCoolDown();
        runSweepIterations(1 + 4);

        verify(sweepIteration, times(1 + 10 * 2 + 4 * 3)).call();
    }

    @Test
    public void doNotExceedMaximumParallelism() throws Exception {
        when(sweepIteration.call()).thenReturn(SUCCESS_LARGE);
        scheduler.start(ScalingSweepTaskScheduler.MAX_PARALLELISM);

        tickClock();
        runSweepIterations(5);
        tickClock();
        runSweepIterations(5);

        verify(sweepIteration, times(10 * MAX_PARALLELISM)).call();
    }

    @Test
    public void tasksNotReducedBeforeCoolDownPasses() throws Exception {
        when(sweepIteration.call()).thenReturn(SUCCESS_SMALL);

        scheduler.start(10);
        runSweepIterations(10);
        verify(sweepIteration, times(10 * 10)).call();
    }

    @Test
    public void withFewEntriesReduceByOneTaskEachTimeCoolDownPasses() throws Exception {
        when(sweepIteration.call()).thenReturn(SUCCESS_SMALL);
        scheduler.start(10);

        tickClock();
        runSweepIterations(1 + 4);

        tickClockForHalfCoolDown();
        runSweepIterations(5);

        tickClockForHalfCoolDown();
        runSweepIterations(1 + 4);

        verify(sweepIteration, times(10 + 10 * 9 + 4 * 8)).call();
    }

    @Test
    public void doNotReduceToZeroTasks() throws Exception {
        when(sweepIteration.call()).thenReturn(SUCCESS_SMALL);
        scheduler.start(1);

        tickClock();
        runSweepIterations(5);
        tickClock();
        runSweepIterations(5);
        verify(sweepIteration, times(10)).call();
    }

    @Test
    public void increaseCanPreventReduction() throws Exception {
        when(sweepIteration.call()).thenReturn(
                SUCCESS_LARGE, SUCCESS_LARGE,
                SUCCESS_SMALL);

        scheduler.start(2);
        tickClock();
        runSweepIterations(1 + 4);

        tickClockForHalfCoolDown();
        runSweepIterations(5);

        tickClockForHalfCoolDown();
        runSweepIterations(1 + 4);

        verify(sweepIteration, times(1 * 2 + 10 * 3 + 4 * 2)).call();
    }

    @Test
    public void attemptedIncreaseCanPreventReduction() throws Exception {
        when(sweepIteration.call()).thenReturn(
                SUCCESS_LARGE, SUCCESS_LARGE,
                SUCCESS_SMALL);

        scheduler.start(2);
        tickClockForHalfCoolDown();
        runSweepIterations(5);

        tickClockForHalfCoolDown();
        runSweepIterations(5);

        tickClockForHalfCoolDown();
        runSweepIterations(1 + 4);

        verify(sweepIteration, times(11 * 2 + 4 * 1)).call();
    }

    @Test
    public void reductionCanPreventIncrease() throws Exception {
        when(sweepIteration.call()).thenReturn(
                SUCCESS_SMALL, SUCCESS_SMALL,
                SUCCESS_LARGE);

        scheduler.start(2);
        tickClock();
        runSweepIterations(1 + 4);

        tickClockForHalfCoolDown();
        runSweepIterations(5);

        tickClockForHalfCoolDown();
        runSweepIterations(1 + 4);

        verify(sweepIteration, times(1 * 2 + 10 * 1 + 4 * 2)).call();
    }

    @Test
    public void attemptedReductionDoesNotPreventIncrease() throws Exception {
        when(sweepIteration.call()).thenReturn(
                SUCCESS_SMALL, SUCCESS_SMALL,
                SUCCESS_LARGE);

        scheduler.start(2);
        tickClockForHalfCoolDown();
        runSweepIterations(5);

        tickClockForHalfCoolDown();
        runSweepIterations(1 + 4);

        tickClockForHalfCoolDown();
        runSweepIterations(5);

        verify(sweepIteration, times(6 * 2 + 9 * 3)).call();
    }

    @Test
    public void whenUnableToAcquireShardReduceTasksIgnoringCoolDown() throws Exception {
        when(sweepIteration.call()).thenReturn(
                SUCCESS_SMALL,
                SUCCESS_MEDIUM,
                SUCCESS_LARGE,
                SweepIterationResults.unableToAcquireShard());

        scheduler.start(3 + 7);
        runSweepIterations(1 + 1);
        verify(sweepIteration, times(10 + 3)).call();
    }

    @Test
    public void whenUnableToAcquireShardOnLastTaskRescheduleAfterMaxPause() throws Exception {
        when(sweepIteration.call()).thenReturn(SweepIterationResults.unableToAcquireShard());

        schedulerWithDelay.start(10);
        runSweepIterations(1 + 9, SweepDelay.DEFAULT_MAX_PAUSE_MILLIS);
        verify(sweepIteration, times(10 + 9 * 1)).call();
    }

    @Test
    public void whenInsufficientConsistencyRescheduleAfterBackoff() throws Exception {
        when(sweepIteration.call()).thenReturn(
                SweepIterationResults.insufficientConsistency(),
                SweepIterationResults.insufficientConsistency(),
                SUCCESS_MEDIUM);

        schedulerWithDelay.start(2);
        runSweepIterations(2, SweepDelay.BACKOFF);
        runSweepIterations(5, DELAY);

        verify(sweepIteration, times(7 * 2)).call();
    }

    @Test
    public void whenDisabledReduceTasksIgnoringCoolDown() throws Exception {
        when(sweepIteration.call()).thenReturn(
                SUCCESS_SMALL,
                SUCCESS_SMALL,
                SUCCESS_SMALL,
                SweepIterationResults.disabled());

        scheduler.start(3 + 7);
        runSweepIterations(1 + 1);
        verify(sweepIteration, times(10 + 3)).call();
    }

    @Test
    public void whenDisabledOnLastTaskRescheduleAfterBackoff() throws Exception {
        when(sweepIteration.call()).thenReturn(SweepIterationResults.disabled());

        schedulerWithDelay.start(10);
        runSweepIterations(1 + 9, SweepDelay.BACKOFF);
        verify(sweepIteration, times(10 + 9 * 1)).call();
    }

    @Test
    public void whenOtherErrorRescheduleAfterMaxPause() throws Exception {
        when(sweepIteration.call()).thenReturn(
                SweepIterationResults.otherError(),
                SweepIterationResults.otherError(),
                SUCCESS_MEDIUM);

        schedulerWithDelay.start(2);
        runSweepIterations(2, SweepDelay.DEFAULT_MAX_PAUSE_MILLIS);
        runSweepIterations(5, DELAY);

        verify(sweepIteration, times(7 * 2)).call();
    }

    @Test
    public void whenVeryFewEntriesIncreasePause() throws Exception {
        SweepDelay sweepDelay = new SweepDelay(100L);
        ScalingSweepTaskScheduler schedulerWithRealDelay = createScheduler(sweepDelay);
        when(sweepIteration.call()).thenReturn(SUCCESS_TINY);

        schedulerWithRealDelay.start(3);
        runSweepIterations(10, 100L);
        verify(sweepIteration, atMost(3 * 10 - 1)).call();
        assertThat(sweepDelay.getNextPause(SUCCESS_MEDIUM)).isGreaterThan(100L);
    }

    @Test
    public void whenVeryManyEntriesDecreasePause() throws Exception {
        SweepDelay sweepDelay = new SweepDelay(100L);
        ScalingSweepTaskScheduler schedulerWithRealDelay = createScheduler(sweepDelay);
        when(sweepIteration.call()).thenReturn(SUCCESS_HUGE);

        schedulerWithRealDelay.start(3);
        runSweepIterations(10, 100L);
        verify(sweepIteration, atLeast(3 * 10 + 1)).call();
        assertThat(sweepDelay.getNextPause(SUCCESS_MEDIUM)).isLessThan(100L);
    }

    @Test
    public void exceptionalIterationsDoNotAffectPause() throws Exception {
        SweepDelay sweepDelay = new SweepDelay(100L);
        ScalingSweepTaskScheduler schedulerWithRealDelay = createScheduler(sweepDelay);
        when(sweepIteration.call()).thenReturn(
                SweepIterationResults.otherError(),
                SweepIterationResults.unableToAcquireShard(),
                SweepIterationResults.otherError(),
                SweepIterationResults.insufficientConsistency());

        schedulerWithRealDelay.start(4);
        runSweepIterations(1);
        verify(sweepIteration, times(4)).call();
        assertThat(sweepDelay.getNextPause(SUCCESS_MEDIUM)).isEqualTo(100L);
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
            assertThat(iterationCount.get()).isLessThanOrEqualTo(100L);

            result.set(SUCCESS_LARGE);
            Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);

            // we have 128 tasks
            result.set(SUCCESS_MEDIUM);
            iterationCount.set(0);
            Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
            assertThat(iterationCount.get()).isGreaterThan(100L);
        }

        nondeterministicScheduler.close();
    }

    private ScalingSweepTaskScheduler createScheduler(SweepDelay sweepDelay) {
        return new ScalingSweepTaskScheduler(
                deterministicScheduler,
                sweepDelay,
                sweepIteration,
                schedulerEnabled::get);
    }

    private void runSweepIterations(int iterations) {
        runSweepIterations(iterations, DELAY);
    }

    private void runSweepIterations(int iterations, long iterationDelay) {
        long duration = (iterations - 1) * iterationDelay + (firstIteration ? INITIAL_DELAY : iterationDelay);
        firstIteration = false;
        deterministicScheduler.tick(duration, TimeUnit.MILLISECONDS);
    }

    private void tickClockForHalfCoolDown() {
        clockMillis.incrementAndGet();
    }

    private void tickClock() {
        clockMillis.incrementAndGet();
        clockMillis.incrementAndGet();
    }
}
