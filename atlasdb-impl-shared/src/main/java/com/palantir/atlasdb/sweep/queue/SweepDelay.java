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

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

class SweepDelay {
    static final int BATCH_CELLS_LOW_THRESHOLD = 100;
    static final long MIN_PAUSE_MILLIS = 1;
    static final long DEFAULT_MAX_PAUSE_MILLIS = 5000;
    static final long BACKOFF = Duration.ofMinutes(2).toMillis();

    private final long initialPause;
    private final long maxPauseMillis;
    private final AtomicLong currentPause;

    /**
     * This class calculates the delay for the next iteration of targeted sweep from the current delay and the outcome
     * of the last iteration of TS. If the sweep iteration was successful, the next delay will gravitate towards the
     * target delay using the formula 0.2 * target + 0.8 * current. The target delay is as follows:
     *
     *  1. if the sweep iteration processed fewer than {@link #BATCH_CELLS_LOW_THRESHOLD} cells, the target pause is
     *  {@link #maxPauseMillis} milliseconds.
     *  2. if the sweep iteration processed a full batch of {@link SweepQueueUtils#SWEEP_BATCH_SIZE} or more cells, the
     *  target pause is {@link #MIN_PAUSE_MILLIS} milliseconds.
     *  3. otherwise, the target pause is {@link #initialPause} milliseconds.
     *
     *  In case of an unsuccessful iteration, the pause is temporarily set to a constant as determined in
     *  {@link #getNextPause(SweepIterationResult)}.
     */
    SweepDelay(long configPause) {
        this.maxPauseMillis = Math.max(DEFAULT_MAX_PAUSE_MILLIS, configPause);
        this.initialPause = Math.max(MIN_PAUSE_MILLIS, configPause);
        this.currentPause = new AtomicLong(initialPause);
    }

    long getNextPause(SweepIterationResult result) {
        return SweepIterationResults.caseOf(result)
                .success(this::updateCurrentPauseAndGet)
                .unableToAcquireShard_(maxPauseMillis)
                .insufficientConsistency_(BACKOFF)
                .otherError_(maxPauseMillis)
                .disabled_(BACKOFF);
    }

    private long updateCurrentPauseAndGet(long numSwept) {
        long target = pauseTarget(numSwept);
        return currentPause.updateAndGet(oldPause -> (4 * oldPause + target) / 5);
    }

    private long pauseTarget(long numSwept) {
        if (numSwept <= BATCH_CELLS_LOW_THRESHOLD) {
            return maxPauseMillis;
        } else if (numSwept >= SweepQueueUtils.SWEEP_BATCH_SIZE) {
            return MIN_PAUSE_MILLIS;
        }
        return initialPause;
    }
}
