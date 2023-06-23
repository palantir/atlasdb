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

import com.palantir.lock.SimpleTimeDuration;
import com.palantir.lock.TimeDuration;
import com.palantir.refreshable.Refreshable;
import com.palantir.refreshable.SettableRefreshable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.Test;

public class AdjustableBackgroundTaskTest {

    /**
     * Dummy deterministic scheduled executor service that cannot be shut down.
     * The default {@link DeterministicScheduler} throws an exception when calling {@link ExecutorService#isShutdown()}.
     */
    private static final class NeverShutdownDeterministicScheduler extends DeterministicScheduler {
        @Override
        public boolean isShutdown() {
            return false;
        }
    }

    // We have to ensure all of our delays are higher than MINIMUM_DELAY_IF_NOT_RUNNING
    // so it doesn't interfere with our tests
    private static final TimeDuration DEFAULT_DELAY = SimpleTimeDuration.of(10, TimeUnit.SECONDS);
    private final AtomicInteger field = new AtomicInteger(0);
    private final SettableRefreshable<Boolean> shouldRun = Refreshable.create(false);
    private final SettableRefreshable<TimeDuration> interval = Refreshable.create(DEFAULT_DELAY);
    private final NeverShutdownDeterministicScheduler scheduledExecutor = new NeverShutdownDeterministicScheduler();
    private final AdjustableBackgroundTask adjustableBackgroundTask =
            new AdjustableBackgroundTask(shouldRun, interval, field::incrementAndGet, scheduledExecutor);

    @Test
    public void doesNotRunTaskDuringConstruction() {
        assertThat(field.get()).isEqualTo(0);
    }

    @Test
    public void doesNotRunTaskByDefault() {
        int before = field.get();
        tick(DEFAULT_DELAY);
        assertThat(field.get()).isEqualTo(before);
    }

    @Test
    public void canBeEnabledDisabledAndReEnabled() {
        int before = field.get();
        shouldRun.update(true);
        tick(DEFAULT_DELAY);
        int after = field.get();
        assertThat(after).isEqualTo(before + 1);

        shouldRun.update(false);
        tick(DEFAULT_DELAY);
        assertThat(field.get()).isEqualTo(after);

        shouldRun.update(true);
        tick(DEFAULT_DELAY);
        assertThat(field.get()).isEqualTo(after + 1);
    }

    @Test
    public void canAdjustDuration() {
        int before = field.get();
        TimeDuration newInterval = SimpleTimeDuration.of(42, TimeUnit.SECONDS);
        interval.update(newInterval);
        shouldRun.update(true);
        tick(SimpleTimeDuration.of(10 * newInterval.toMillis(), TimeUnit.MILLISECONDS));
        assertThat(field.get()).isEqualTo(before + 10);
    }

    private void tick(TimeDuration duration) {
        scheduledExecutor.tick(duration.toMillis(), TimeUnit.MILLISECONDS);
    }
}
