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

import com.palantir.timelock.corruption.detection.TimeLockCorruptionTestSetup.StateLogComponents;
import com.palantir.timelock.history.utils.PaxosSerializationTestUtils;
import java.util.stream.IntStream;
import org.junit.Rule;
import org.junit.Test;

public class LocalTimestampInvariantsVerifierTest {
    @Rule
    public TimeLockCorruptionDetectionHelper helper = new TimeLockCorruptionDetectionHelper();

    @Test
    public void isHealthyIfTimestampsAreIncreasing() {
        // write increasing timestamp values in range [1, LEARNER_LOG_BATCH_SIZE_LIMIT - 1]
        helper.writeLogsOnDefaultLocalServer(1, LEARNER_LOG_BATCH_SIZE_LIMIT - 1);
        helper.assertLocalTimestampInvariantsStandInNextBatch();
    }

    @Test
    public void isHealthyIfTimestampsAreNonDecreasing() {
        StateLogComponents localServer = helper.getDefaultLocalServer();
        // write a constant timestamp value in range [1, LEARNER_LOG_BATCH_SIZE_LIMIT - 1]
        IntStream.rangeClosed(1, LEARNER_LOG_BATCH_SIZE_LIMIT - 1)
                .boxed()
                .forEach(round -> PaxosSerializationTestUtils.writePaxosValue(
                        localServer.learnerLog(),
                        round,
                        PaxosSerializationTestUtils.createPaxosValueForRoundAndData(round, 5)));
        helper.assertLocalTimestampInvariantsStandInNextBatch();
    }

    @Test
    public void detectsClockWentBackwardsOnNode() {
        helper.writeLogsOnDefaultLocalServer(1, LEARNER_LOG_BATCH_SIZE_LIMIT - 1);
        helper.createTimestampInversion(5);
        helper.assertClockGoesBackwardsInNextBatch();
    }

    @Test
    public void detectsClockWentBackwardsForDiscontinuousLogs() {
        helper.writeLogsOnDefaultLocalServer(1, 27);
        helper.writeLogsOnDefaultLocalServer(LEARNER_LOG_BATCH_SIZE_LIMIT, LEARNER_LOG_BATCH_SIZE_LIMIT + 97);

        helper.createTimestampInversion(72);
        helper.assertClockGoesBackwardsInNextBatch();
    }

    @Test
    public void detectsClockWentBackwardsAtBatchStart() {
        helper.writeLogsOnDefaultLocalServer(1, LEARNER_LOG_BATCH_SIZE_LIMIT);
        helper.createTimestampInversion(1);
        helper.assertClockGoesBackwardsInNextBatch();
    }

    @Test
    public void detectsClockWentBackwardsAtBatchEnd() {
        helper.writeLogsOnDefaultLocalServer(1, LEARNER_LOG_BATCH_SIZE_LIMIT);
        helper.createTimestampInversion(LEARNER_LOG_BATCH_SIZE_LIMIT - 1);
        helper.assertClockGoesBackwardsInNextBatch();
    }

    @Test
    public void detectsClockWentBackwardsStartOfNextBatch() {
        helper.writeLogsOnDefaultLocalServer(1, 2 * LEARNER_LOG_BATCH_SIZE_LIMIT);
        helper.createTimestampInversion(LEARNER_LOG_BATCH_SIZE_LIMIT);

        // No signs of corruption in the first batch
        helper.assertLocalTimestampInvariantsStandInNextBatch();

        // Detects signs of corruption in the second batch
        helper.assertClockGoesBackwardsInNextBatch();
    }

    @Test
    public void detectsClockWentBackwardsInLaterBatch() {
        helper.writeLogsOnDefaultLocalAndRemote(1, 2 * LEARNER_LOG_BATCH_SIZE_LIMIT);
        helper.createTimestampInversion(3 * LEARNER_LOG_BATCH_SIZE_LIMIT / 2);

        // No signs of corruption in the first batch
        helper.assertLocalTimestampInvariantsStandInNextBatch();

        // Detects signs of corruption in the second batch
        helper.assertClockGoesBackwardsInNextBatch();
    }

    @Test
    public void resetsProgressIfNotEnoughLogsForVerification() {
        helper.createTimestampInversion(1);

        // No signs of corruption since there aren't enough logs
        helper.assertLocalTimestampInvariantsStandInNextBatch();

        helper.writeLogsOnDefaultLocalServer(2, LEARNER_LOG_BATCH_SIZE_LIMIT);

        // Detects signs of corruption in the now corrupt first batch of logs
        helper.assertClockGoesBackwardsInNextBatch();
    }
}
