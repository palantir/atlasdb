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

package com.palantir.timelock.corruption.detection;

import static com.palantir.timelock.corruption.detection.LocalTimestampInvariantsVerifier.DELTA;
import static com.palantir.timelock.history.PaxosLogHistoryProgressTracker.MAX_ROWS_ALLOWED;

import org.junit.Rule;
import org.junit.Test;

public class LocalTimestampInvariantsVerifierTest {
    @Rule
    public TimeLockCorruptionDetectionHelper helper = new TimeLockCorruptionDetectionHelper();

    @Test
    public void detectCorruptionIfClockWentBackwardsOnNode() {
        helper.writeLogsOnDefaultLocalServer(1, MAX_ROWS_ALLOWED - 1);
        helper.forceTimestampToGoBackwards(5);
        helper.assertClockWentBackwards();
    }

    @Test
    public void detectIfClockWentBackwardsAtBatchEnd() {
        helper.writeLogsOnDefaultLocalServer(1, MAX_ROWS_ALLOWED);
        helper.forceTimestampToGoBackwards(MAX_ROWS_ALLOWED - 1);
        helper.assertClockWentBackwards();
    }

    @Test
    public void detectIfClockWentBackwardsInLaterBatch() {
        helper.writeLogsOnDefaultLocalAndRemote(1, MAX_ROWS_ALLOWED * 2);
        helper.forceTimestampToGoBackwards(MAX_ROWS_ALLOWED + DELTA + 1);

        // No signs of corruption in the first batch
        helper.assertLocalTimestampInvariantsStand();

        // Detects signs of corruption in the second batch
        helper.assertClockWentBackwards();
    }
}
