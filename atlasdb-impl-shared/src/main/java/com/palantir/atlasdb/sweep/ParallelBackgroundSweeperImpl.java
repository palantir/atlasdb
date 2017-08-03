/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.sweep;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Supplier;
import com.palantir.common.concurrent.NamedThreadFactory;

public final class ParallelBackgroundSweeperImpl {

    private final Supplier<Boolean> isSweepEnabled;
    private final Supplier<Long> sweepPauseMillis;
    private final SpecificTableSweeper specificTableSweeper;
    private final int numberOfConcurrentSweeps;
    private final ScheduledExecutorService executorService;
    private final AtomicInteger numberOfSweepsRunning;

    private ParallelBackgroundSweeperImpl(
            Supplier<Boolean> isSweepEnabled,
            Supplier<Long> sweepPauseMillis,
            SpecificTableSweeper specificTableSweeper,
            int numberOfConcurrentSweeps,
            ScheduledExecutorService executorService) {
        this.isSweepEnabled = isSweepEnabled;
        this.sweepPauseMillis = sweepPauseMillis;
        this.specificTableSweeper = specificTableSweeper;
        this.numberOfConcurrentSweeps = numberOfConcurrentSweeps;
        this.executorService = executorService;
        this.numberOfSweepsRunning = new AtomicInteger(0);
    }

    public static ParallelBackgroundSweeperImpl create(
            Supplier<Boolean> isSweepEnabled,
            Supplier<Long> sweepPauseMillis,
            SpecificTableSweeper specificTableSweeper,
            int numberOfConcurrentSweeps) {
        return new ParallelBackgroundSweeperImpl(
                isSweepEnabled,
                sweepPauseMillis,
                specificTableSweeper,
                numberOfConcurrentSweeps,
                Executors.newScheduledThreadPool(numberOfConcurrentSweeps,
                        new NamedThreadFactory("BackgroundSweeper", true)));
    }

    public void runInBackground() {
        for (int i = 0; i < numberOfConcurrentSweeps; i++) {
            BackgroundSweeperImpl sweeper = BackgroundSweeperImpl.create(createSweepLocks(),
                    isSweepEnabled,
                    specificTableSweeper);
            executorService.scheduleAtFixedRate(
                    sweeper,
                    getBackoffTimeForFirstRun(),
                    getBackoffTimeBetweenSweepRuns(),
                    TimeUnit.MILLISECONDS);
        }
    }

    public boolean isSweepRunning() {
        return numberOfSweepsRunning.get() > 0;
    }

    private SweepLocks createSweepLocks() {
        return new SweepLocks(specificTableSweeper.getTxManager().getLockService(), numberOfConcurrentSweeps,
                numberOfSweepsRunning);
    };

    private long getBackoffTimeForFirstRun() {
        return 20 * (1000 + sweepPauseMillis.get());
    }

    private long getBackoffTimeBetweenSweepRuns() {
        return sweepPauseMillis.get();
    }
}
