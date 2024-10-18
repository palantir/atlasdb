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

import static com.palantir.logsafe.testing.Assertions.assertThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.palantir.atlasdb.sweep.asts.locks.Lockable;
import com.palantir.atlasdb.sweep.asts.locks.Lockable.LockedItem;
import com.palantir.atlasdb.sweep.asts.locks.LockableFactory;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;
import com.palantir.refreshable.Refreshable;
import com.palantir.refreshable.SettableRefreshable;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.jupiter.api.Test;

public final class DynamicTaskSchedulerTest {
    private final DeterministicScheduler scheduler = new DeterministicScheduler();
    private final SettableRefreshable<Duration> taskDelay = Refreshable.create(Duration.ofSeconds(1));

    private final AtomicInteger taskRunCount = new AtomicInteger(0);
    private final Runnable task = taskRunCount::incrementAndGet;

    private final DynamicTaskScheduler taskRunner = DynamicTaskScheduler.create(scheduler, taskDelay, task, "testTask");

    @Test
    public void doesNotRunTaskBeforeStarted() {
        tick(Duration.ofDays(1));
        assertThat(taskRunCount.get()).isEqualTo(0);
    }

    @Test
    public void runsFirstTaskAfterInitialDelayOnceStarted() {
        taskRunner.start();
        tick(Duration.ofMillis(999));
        assertThat(taskRunCount.get()).isEqualTo(0);

        tick(Duration.ofMillis(2));
        assertThat(taskRunCount.get()).isEqualTo(1);
    }

    @Test
    public void runsSubsequentTasksWithUpdatedDelaysOnNextSchedule() {
        taskRunner.start();
        taskDelay.update(Duration.ofSeconds(2));
        tick(Duration.ofMillis(1001));
        assertThat(taskRunCount.get())
                .as("Delay was 1 second, so we should have run the task")
                .isEqualTo(1);

        tick(Duration.ofMillis(1001));
        assertThat(taskRunCount.get())
                .as("Delay is now 2 seconds, but <2s has passed since the last invocation, so we shouldn't have"
                        + " run the task")
                .isEqualTo(1);

        taskDelay.update(Duration.ofMillis(100));
        tick(Duration.ofSeconds(1));
        assertThat(taskRunCount.get())
                .as("More than 2 seconds have passed since the last invocation, so the task should now have run")
                .isEqualTo(2);

        tick(Duration.ofMillis(100));
        assertThat(taskRunCount.get())
                .as("Delay is now 100 milliseconds, so we should have run the task")
                .isEqualTo(3);
    }

    @Test
    public void furtherIterationsOccurEvenWhenTaskFails() {
        Runnable runnable = mock(Runnable.class);
        DynamicTaskScheduler newRunner = DynamicTaskScheduler.create(scheduler, taskDelay, runnable, "testTask");
        newRunner.start();
        tick(Duration.ofSeconds(1));
        verify(runnable).run();

        doThrow(new RuntimeException()).when(runnable).run();
        tick(Duration.ofSeconds(1));
        // Task is called, but will fail
        verify(runnable, times(2)).run();

        doNothing().when(runnable).run();
        tick(Duration.ofSeconds(1));
        // Task is called again, despite the previous iteration failing.
        verify(runnable, times(3)).run();
    }

    @Test
    public void lockableExclusiveTaskDoesNotRunWhenLockCannotBeAcquired() {
        Lockable<ExclusiveTask> lockableTask = setupExclusiveTask();
        DynamicTaskScheduler taskRunner =
                DynamicTaskScheduler.createForExclusiveTask(scheduler, taskDelay, lockableTask, "testTask");
        Optional<LockedItem<ExclusiveTask>> locked = lockableTask.tryLock(_ignored -> {});
        assertThat(locked).isPresent();

        taskRunner.start();
        tick(Duration.ofSeconds(1));
        assertThat(taskRunCount.get()).isEqualTo(0);
    }

    @Test
    public void lockableExclusiveTaskRunsWhenLockIsAcquirableAndReleasesLockAfterwards() {
        Lockable<ExclusiveTask> lockableTask = setupExclusiveTask();
        DynamicTaskScheduler taskRunner =
                DynamicTaskScheduler.createForExclusiveTask(scheduler, taskDelay, lockableTask, "testTask");

        taskRunner.start();
        tick(Duration.ofSeconds(1));
        assertThat(taskRunCount.get()).isEqualTo(1);

        // Try acquire the lock afterwards, which would fail if it wasn't released
        Optional<LockedItem<ExclusiveTask>> locked = lockableTask.tryLock(_ignored -> {});
        assertThat(locked).isPresent();
    }

    private Lockable<ExclusiveTask> setupExclusiveTask() {
        TimelockService timelockService = mock(TimelockService.class);
        when(timelockService.lock(LockRequest.of(Set.of(StringLockDescriptor.of("test")), 1)))
                .thenReturn(LockResponse.successful(LockToken.of(UUID.randomUUID())));
        LockableFactory<ExclusiveTask> factory = LockableFactory.create(
                timelockService,
                Refreshable.only(Duration.ofMillis(1)),
                task -> StringLockDescriptor.of(task.safeLockDescriptor()));
        ExclusiveTask task = ImmutableExclusiveTask.builder()
                .safeLockDescriptor("test")
                .task(taskRunCount::incrementAndGet)
                .build();
        return factory.createLockable(task);
    }

    private void tick(Duration duration) {
        scheduler.tick(duration.toMillis(), TimeUnit.MILLISECONDS);
    }
}
