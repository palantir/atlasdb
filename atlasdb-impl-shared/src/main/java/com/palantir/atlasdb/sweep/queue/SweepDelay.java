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

public class SweepDelay {
    public static final int BATCH_CELLS_LOW_THRESHOLD = 100;
    public static final long MIN_PAUSE_MILLIS = 1;
    public static final long MAX_PAUSE_MILLIS = 5000;
    public static final long BACKOFF = Duration.ofMinutes(2).toMillis();

    private final long initialPause;
    private final AtomicLong currentPause;

    public SweepDelay(long configPause) {
        this.initialPause = Math.min(Math.max(MIN_PAUSE_MILLIS, configPause), MAX_PAUSE_MILLIS);
        this.currentPause = new AtomicLong(initialPause);
    }

    public long getNextPause(SweepIterationResult result) {
        return SweepIterationResults.caseOf(result)
                .success(this::updateCurrentPauseAndGet)
                .unableToAcquireShard_(MAX_PAUSE_MILLIS)
                .insufficientConsistency_(BACKOFF)
                .otherError_(MAX_PAUSE_MILLIS)
                .disabled_(BACKOFF);
    }

    private long updateCurrentPauseAndGet(long numSwept) {
        long target = pauseTarget(numSwept);
        return currentPause.updateAndGet(oldPause -> (4 * oldPause + target) / 5);
    }

    private long pauseTarget(long numSwept) {
        if (numSwept <= BATCH_CELLS_LOW_THRESHOLD) {
            return MAX_PAUSE_MILLIS;
        } else if (numSwept >= SweepQueueUtils.SWEEP_BATCH_SIZE) {
            return MIN_PAUSE_MILLIS;
        }
        return initialPause;
    }
}
