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

import static com.palantir.timelock.corruption.detection.LocalTimestampInvariantsVerifier.LEARNER_LOG_BATCH_SIZE_LIMIT;

import org.junit.Rule;
import org.junit.Test;

public class LocalTimestampInvariantsVerifierTest {
    @Rule
    public TimeLockCorruptionDetectionHelper helper = new TimeLockCorruptionDetectionHelper();

    @Test
    public void detectCorruptionIfClockWentBackwardsOnNode() {
        helper.writeLogsOnDefaultLocalServer(1, LEARNER_LOG_BATCH_SIZE_LIMIT - 1);
        helper.forceTimestampToGoBackwards(5);
        helper.assertClockWentBackwards();
    }

    @Test
    public void detectIfClockWentBackwardsAtBatchStart() {
        helper.writeLogsOnDefaultLocalServer(1, LEARNER_LOG_BATCH_SIZE_LIMIT);
        helper.forceTimestampToGoBackwards(1);
        helper.assertClockWentBackwards();
    }

    @Test
    public void detectIfClockWentBackwardsAtBatchEnd() {
        helper.writeLogsOnDefaultLocalServer(1, LEARNER_LOG_BATCH_SIZE_LIMIT);
        helper.forceTimestampToGoBackwards(LEARNER_LOG_BATCH_SIZE_LIMIT / 2);
        helper.assertClockWentBackwards();
    }

    @Test
    public void detectIfClockWentBackwardsStartOfNextBatch() {
        helper.writeLogsOnDefaultLocalServer(1, 2 * LEARNER_LOG_BATCH_SIZE_LIMIT);
        helper.forceTimestampToGoBackwards(LEARNER_LOG_BATCH_SIZE_LIMIT);

        // No signs of corruption in the first batch
        helper.assertLocalTimestampInvariantsStand();

        // Detects signs of corruption in the second batch
        helper.assertClockWentBackwards();
    }

    @Test
    public void detectIfClockWentBackwardsInLaterBatch() {
        helper.writeLogsOnDefaultLocalAndRemote(1, 2 * LEARNER_LOG_BATCH_SIZE_LIMIT);
        helper.forceTimestampToGoBackwards(3 * LEARNER_LOG_BATCH_SIZE_LIMIT / 2);

        // No signs of corruption in the first batch
        helper.assertLocalTimestampInvariantsStand();

        // Detects signs of corruption in the second batch
        helper.assertClockWentBackwards();
    }

    @Test
    public void resetsProgressIfNotEnoughLogsForVerification() {
        helper.writeLogsOnDefaultLocalServer(1, 1);
        // No signs of corruption
        helper.assertLocalTimestampInvariantsStand();

        helper.writeLogsOnDefaultLocalServer(2, LEARNER_LOG_BATCH_SIZE_LIMIT);
        helper.forceTimestampToGoBackwards(LEARNER_LOG_BATCH_SIZE_LIMIT / 2);

        // Detects signs of corruption in the now corrupt first batch of logs
        helper.assertClockWentBackwards();
    }
}
