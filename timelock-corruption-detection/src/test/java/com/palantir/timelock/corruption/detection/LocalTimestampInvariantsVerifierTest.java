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

import static com.palantir.timelock.history.PaxosLogHistoryProgressTracker.MAX_ROWS_ALLOWED;
import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.timelock.history.utils.PaxosSerializationTestUtils;
import org.junit.Rule;
import org.junit.Test;

public class LocalTimestampInvariantsVerifierTest {
    @Rule
    public TimeLockCorruptionDetectionHelper helper = new TimeLockCorruptionDetectionHelper();

    @Test
    public void detectCorruptionIfClockWentBackwardsOnNode() {
        helper.writeLogsOnDefaultLocalServer(1, MAX_ROWS_ALLOWED - 1);
        PaxosSerializationTestUtils.writePaxosValue(
                helper.getDefaultLocalServer().learnerLog(),
                5,
                PaxosSerializationTestUtils.createPaxosValueForRoundAndData(5, 10));
        CorruptionHealthReport corruptionHealthReport =
                helper.getLocalTimestampInvariantsVerifier().timestampInvariantsHealthReport();
        assertThat(corruptionHealthReport.violatingStatusesToNamespaceAndUseCase().keySet())
                .containsExactly(CorruptionCheckViolation.CLOCK_WENT_BACKWARDS);
    }
}
