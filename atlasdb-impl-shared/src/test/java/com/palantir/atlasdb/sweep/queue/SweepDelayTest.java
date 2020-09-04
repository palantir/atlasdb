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


import static com.palantir.atlasdb.sweep.queue.SweepDelay.BACKOFF;
import static com.palantir.atlasdb.sweep.queue.SweepDelay.BATCH_CELLS_LOW_THRESHOLD;
import static com.palantir.atlasdb.sweep.queue.SweepDelay.DEFAULT_MAX_PAUSE_MILLIS;
import static com.palantir.atlasdb.sweep.queue.SweepDelay.MIN_PAUSE_MILLIS;
import static com.palantir.atlasdb.sweep.queue.SweepQueueUtils.SWEEP_BATCH_SIZE;
import static com.palantir.logsafe.testing.Assertions.assertThat;

import org.junit.Test;

public class SweepDelayTest {
    private static final SweepIterationResult SUCCESS_TOO_FAST = SweepIterationResults.success(1L);
    private static final SweepIterationResult SUCCESS_TOO_SLOW = SweepIterationResults.success(SWEEP_BATCH_SIZE);
    private static final SweepIterationResult SUCCESS = SweepIterationResults
            .success((BATCH_CELLS_LOW_THRESHOLD + SWEEP_BATCH_SIZE) / 2);
    private static final long INITIAL_DELAY = 250L;

    private SweepDelay delay = new SweepDelay(INITIAL_DELAY);

    @Test
    public void iterationWithNormalBatchReturnsInitialPause() {
        assertThat(delay.getNextPause(SUCCESS)).isEqualTo(INITIAL_DELAY);
    }

    @Test
    public void configurationBelowMinimumIsSetToMinimum() {
        SweepDelay negativeDelay = new SweepDelay(-5L);

        assertThat(negativeDelay.getNextPause(SUCCESS)).isEqualTo(MIN_PAUSE_MILLIS);
    }

    @Test
    public void configurationAboveDefaultMaximumIsRespected() {
        SweepDelay largeDelay = new SweepDelay(2 * DEFAULT_MAX_PAUSE_MILLIS);

        assertThat(largeDelay.getNextPause(SUCCESS)).isEqualTo(2 * DEFAULT_MAX_PAUSE_MILLIS);
    }

    @Test
    public void unableToAcquireShardReturnsMaxPause() {
        assertThat(delay.getNextPause(SweepIterationResults.unableToAcquireShard()))
                .isEqualTo(DEFAULT_MAX_PAUSE_MILLIS);
    }

    @Test
    public void insufficientConsistencyReturnsBackoff() {
        assertThat(delay.getNextPause(SweepIterationResults.insufficientConsistency())).isEqualTo(BACKOFF);
    }

    @Test
    public void otherErrorReturnsMaxPause() {
        assertThat(delay.getNextPause(SweepIterationResults.otherError())).isEqualTo(DEFAULT_MAX_PAUSE_MILLIS);
    }

    @Test
    public void disabledReturnsBackoff() {
        assertThat(delay.getNextPause(SweepIterationResults.disabled())).isEqualTo(BACKOFF);
    }

    @Test
    public void iterationWithSmallBatchIncreasesPause() {
        assertThat(delay.getNextPause(SUCCESS_TOO_FAST)).isGreaterThan(INITIAL_DELAY);
    }

    @Test
    public void iterationWithFullBatchReducesPause() {
        assertThat(delay.getNextPause(SUCCESS_TOO_SLOW)).isLessThan(INITIAL_DELAY);
    }

    @Test
    public void consistentSmallBatchesGravitatesTowardsMaximumPause() {
        sweepTwentyIterationsWithResult(SUCCESS_TOO_FAST);
        assertThat(delay.getNextPause(SUCCESS_TOO_FAST))
                .isGreaterThanOrEqualTo((long) (DEFAULT_MAX_PAUSE_MILLIS * 0.95));
    }

    @Test
    public void consistentFullBatchesGravitatesTowardsMinimumPause() {
        sweepTwentyIterationsWithResult(SUCCESS_TOO_SLOW);
        assertThat(delay.getNextPause(SUCCESS_TOO_SLOW)).isGreaterThanOrEqualTo((long) (MIN_PAUSE_MILLIS * 1.05));
    }

    @Test
    public void consistentNormalBatchesAfterFullBatchesGravitatesTowardsInitialPause() {
        sweepTwentyIterationsWithResult(SUCCESS_TOO_SLOW);
        sweepTwentyIterationsWithResult(SUCCESS);
        long nextPause = delay.getNextPause(SUCCESS);
        assertThat(nextPause).isGreaterThanOrEqualTo((long) (INITIAL_DELAY * 0.95));
        assertThat(nextPause).isLessThanOrEqualTo((long) (INITIAL_DELAY * 1.05));

    }

    private void sweepTwentyIterationsWithResult(SweepIterationResult result) {
        for (int i = 0; i < 20; i++) {
            delay.getNextPause(result);
        }
    }
}
