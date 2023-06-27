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

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.refreshable.Refreshable;
import com.palantir.refreshable.SettableRefreshable;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.BeforeClass;
import org.junit.Test;

public class AdjustableBackgroundTaskTest {
    private static final Duration DEFAULT_INTERVAL = Duration.ofSeconds(10);
    private final AtomicInteger field = new AtomicInteger(0);
    private final SettableRefreshable<Boolean> shouldRun = Refreshable.create(false);
    private final SettableRefreshable<Duration> interval = Refreshable.create(DEFAULT_INTERVAL);
    private final DeterministicScheduler scheduledExecutor = new DeterministicScheduler();
    private final AtomicInteger numCallsToShouldRunSupplier = new AtomicInteger();
    private final AtomicInteger numCallsToIntervalSupplier = new AtomicInteger(0);
    private final AdjustableBackgroundTask adjustableBackgroundTask = new AdjustableBackgroundTask(
            () -> {
                numCallsToShouldRunSupplier.incrementAndGet();
                return shouldRun.current();
            },
            () -> {
                numCallsToIntervalSupplier.incrementAndGet();
                return interval.current();
            },
            field::incrementAndGet,
            scheduledExecutor);

    @BeforeClass
    public static void testDelayShouldNotExceedMinimumDelay() {
        // We have to ensure that our default interval is higher than MINIMUM_INTERVAL_IF_NOT_RUNNING
        // so our tests are actually meaningful
        assertThat(DEFAULT_INTERVAL).isGreaterThanOrEqualTo(AdjustableBackgroundTask.MINIMUM_INTERVAL_IF_NOT_RUNNING);
    }

    @Test
    public void doesNotRunTaskDuringConstruction() {
        assertThat(field.get()).isEqualTo(0);
    }

    @Test
    public void doesNotRunTaskByDefault() {
        tick(DEFAULT_INTERVAL);
        assertThat(field.get()).isEqualTo(0);
    }

    @Test
    public void canBeEnabledDisabledAndReEnabled() {
        shouldRun.update(true);
        tick(DEFAULT_INTERVAL);
        int after = field.get();
        assertThat(after).isEqualTo(1);

        shouldRun.update(false);
        tick(DEFAULT_INTERVAL);
        assertThat(field.get()).isEqualTo(after);

        shouldRun.update(true);
        tick(DEFAULT_INTERVAL);
        assertThat(field.get()).isEqualTo(after + 1);
    }

    @Test
    public void canAdjustInterval() {
        Duration newInterval = Duration.ofMinutes(1);
        interval.update(newInterval);
        shouldRun.update(true);
        // will run the task once
        tick(DEFAULT_INTERVAL);
        tick(Duration.ofMinutes(42 - 1));
        assertThat(field.get()).isEqualTo(42);
    }

    @Test
    public void canAdjustIntervalBelowMinimumDelayIfRunning() {
        Duration newInterval = Duration.ofMillis(1);
        interval.update(newInterval);
        shouldRun.update(true);
        // will run the task once
        tick(DEFAULT_INTERVAL);
        tick(Duration.ofMillis(10 - 1));
        assertThat(field.get()).isEqualTo(10);
    }

    @Test
    public void runsSuppliersAtReducedIntervalIfNotRunning() {
        Duration newInterval = Duration.ofMillis(1);
        interval.update(newInterval);
        Duration elapsedDuration = Duration.ofSeconds(10);
        // will invoke suppliers an additional time
        tick(DEFAULT_INTERVAL);
        tick(elapsedDuration);
        assertThat(field.get()).isEqualTo(0);
        // suppliers are also called once when running the constructor
        assertThat(numCallsToShouldRunSupplier.get())
                .isEqualTo(elapsedDuration.dividedBy(AdjustableBackgroundTask.MINIMUM_INTERVAL_IF_NOT_RUNNING) + 2);
        assertThat(numCallsToIntervalSupplier.get())
                .isEqualTo(elapsedDuration.dividedBy(AdjustableBackgroundTask.MINIMUM_INTERVAL_IF_NOT_RUNNING) + 2);
    }

    @Test
    public void suppliersAreCalledTogetherWithTask() {
        shouldRun.update(true);
        tick(Duration.ofSeconds(10 * DEFAULT_INTERVAL.toSeconds()));
        assertThat(field.get()).isEqualTo(10);
        // suppliers are also called once when running the constructor
        assertThat(numCallsToShouldRunSupplier.get()).isEqualTo(11);
        assertThat(numCallsToIntervalSupplier.get()).isEqualTo(11);
    }

    private void tick(Duration duration) {
        scheduledExecutor.tick(duration.toMillis(), TimeUnit.MILLISECONDS);
    }
}
