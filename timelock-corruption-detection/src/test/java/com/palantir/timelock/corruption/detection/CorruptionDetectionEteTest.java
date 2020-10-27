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

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.SetMultimap;
import com.palantir.paxos.NamespaceAndUseCase;
import org.junit.Test;

public class CorruptionDetectionEteTest extends CorruptionDetectionEteSetup {

    @Test
    public void detectCorruptionForLogAtSeqAtBatchEnd() {
        induceGreaterAcceptedValueCorruption(1, 500, 499);

        SetMultimap<CorruptionCheckViolation, NamespaceAndUseCase> violationsToNamespaceToUseCaseMultimap =
                getViolationsToNamespaceToUseCaseMultimap();
        assertThat(violationsToNamespaceToUseCaseMultimap.keySet())
                .containsExactly(CorruptionCheckViolation.ACCEPTED_VALUE_GREATER_THAN_LEARNED);
    }

    @Test
    public void detectCorruptionForLogSeqInLaterBatches() {
        induceGreaterAcceptedValueCorruption(1, 1000, 599);

        // No signs of corruption in the first batch
        SetMultimap<CorruptionCheckViolation, NamespaceAndUseCase> violationsToNamespaceToUseCaseMultimap =
                getViolationsToNamespaceToUseCaseMultimap();
        assertThat(violationsToNamespaceToUseCaseMultimap.isEmpty()).isTrue();

        // Detects signs of corruption in the second batch
        SetMultimap<CorruptionCheckViolation, NamespaceAndUseCase> nextBatchViolationsToNamespaceToUseCaseMultimap =
                getViolationsToNamespaceToUseCaseMultimap();
        assertThat(nextBatchViolationsToNamespaceToUseCaseMultimap.keySet())
                .containsExactly(CorruptionCheckViolation.ACCEPTED_VALUE_GREATER_THAN_LEARNED);
    }
}
