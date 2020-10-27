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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;
import com.palantir.paxos.Client;
import com.palantir.paxos.ImmutableNamespaceAndUseCase;
import com.palantir.paxos.NamespaceAndUseCase;
import java.util.List;
import java.util.stream.IntStream;
import org.junit.Test;

// Note - All tests only induce and detect one type of corruption check violation ->
// CorruptionCheckViolation.ACCEPTED_VALUE_GREATER_THAN_LEARNED
public class CorruptionDetectionIntegrationTest extends TimeLockCorruptionTestSetup {

    @Test
    public void detectCorruptionForLogAtSeqAtBatchEnd() {
        // We write logs in range - [1, 500]. The first range of sequences for corruption detection = [0, 499] since
        // this range is computed from INITIAL_PROGRESS = -1.
        writeLogsOnLocalAndRemote(1, 500);
        induceGreaterAcceptedValueCorruption(499);
        assertDetectedViolations(ImmutableSet.of(CorruptionCheckViolation.ACCEPTED_VALUE_GREATER_THAN_LEARNED));
    }

    @Test
    public void detectCorruptionForLogSeqInLaterBatches() {
        writeLogsOnLocalAndRemote(1, 1000);
        induceGreaterAcceptedValueCorruption(599);

        // No signs of corruption in the first batch
        SetMultimap<CorruptionCheckViolation, NamespaceAndUseCase> violationsToNamespaceToUseCaseMultimap =
                getViolationsToNamespaceToUseCaseMultimap();
        assertThat(violationsToNamespaceToUseCaseMultimap.isEmpty()).isTrue();

        // Detects signs of corruption in the second batch
        assertDetectedViolations(ImmutableSet.of(CorruptionCheckViolation.ACCEPTED_VALUE_GREATER_THAN_LEARNED));
    }

    @Test
    public void detectCorruptionForLogSeqAtStartOfSecondBatch() {
        // We write logs in range - [1, 1000]. The first range of sequences for corruption detection = [0, 499] since
        // this range is computed from INITIAL_PROGRESS = -1, which makes range of the second batch = [500, 999].
        writeLogsOnLocalAndRemote(1, 1000);
        induceGreaterAcceptedValueCorruption(500);

        // No signs of corruption in the first batch
        SetMultimap<CorruptionCheckViolation, NamespaceAndUseCase> violationsToNamespaceToUseCaseMultimap =
                getViolationsToNamespaceToUseCaseMultimap();
        assertThat(violationsToNamespaceToUseCaseMultimap.isEmpty()).isTrue();

        // Detects signs of corruption in the second batch
        assertDetectedViolations(ImmutableSet.of(CorruptionCheckViolation.ACCEPTED_VALUE_GREATER_THAN_LEARNED));
    }

    @Test
    public void resetsLastVerifiedOnceGreatestKnownSeqInMemoryIsVerified() {
        writeLogsOnLocalAndRemote(1, 400);

        // No signs of corruption
        assertDetectedViolations(ImmutableSet.of(), ImmutableSet.of());

        induceGreaterAcceptedValueCorruption(250);
        // Detects signs of corruption in the now corrupt first batch of logs
        assertDetectedViolations(ImmutableSet.of(CorruptionCheckViolation.ACCEPTED_VALUE_GREATER_THAN_LEARNED));
    }

    @Test
    public void detectCorruptionForMultipleCorruptSeries() {
        // We create 7 series and write logs to each of these in the range [1, 500]. We then corrupt series 6 & 7.
        IntStream.rangeClosed(1, 7).boxed().forEach(ind -> createSeriesWithPaxosLogs(ind));
        IntStream.rangeClosed(6, 7).boxed().forEach(ind -> corruptSeries(ind));

        assertDetectedViolations(
                ImmutableSet.of(CorruptionCheckViolation.ACCEPTED_VALUE_GREATER_THAN_LEARNED),
                ImmutableSet.of(namespaceAndUseCaseForIndex(6), namespaceAndUseCaseForIndex(7)));
    }

    private void createSeriesWithPaxosLogs(Integer namespaceAndUseCaseIndex) {
        NamespaceAndUseCase namespaceAndUseCase = namespaceAndUseCaseForIndex(namespaceAndUseCaseIndex);
        writeLogsOnLocalAndRemote(createStatLogComponentsForNamespaceAndUseCase(namespaceAndUseCase), 1, 500);
    }

    private void corruptSeries(Integer namespaceAndUseCaseIndex) {
        NamespaceAndUseCase namespaceAndUseCase = namespaceAndUseCaseForIndex(namespaceAndUseCaseIndex);
        List<StateLogComponents> components = createStatLogComponentsForNamespaceAndUseCase(namespaceAndUseCase);
        induceGreaterAcceptedValueCorruption(components.get(0), 499);
    }

    private NamespaceAndUseCase namespaceAndUseCaseForIndex(Integer ind) {
        return ImmutableNamespaceAndUseCase.of(Client.of("client_" + ind), "client");
    }
}
