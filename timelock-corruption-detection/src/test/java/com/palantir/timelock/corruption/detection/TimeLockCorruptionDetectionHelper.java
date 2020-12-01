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
import com.google.common.collect.Multimap;
import com.palantir.paxos.NamespaceAndUseCase;
import com.palantir.timelock.TimelockCorruptionTestConstants;
import com.palantir.timelock.corruption.detection.TimeLockCorruptionTestSetup.StateLogComponents;
import com.palantir.timelock.history.models.CompletePaxosHistoryForNamespaceAndUseCase;
import com.palantir.timelock.history.utils.PaxosSerializationTestUtils;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public final class TimeLockCorruptionDetectionHelper implements TestRule {
    private TimeLockCorruptionTestSetup timeLockCorruptionTestSetup = new TimeLockCorruptionTestSetup();

    @Override
    public Statement apply(Statement base, Description description) {
        return timeLockCorruptionTestSetup.apply(base, description);
    }

    void writeLogsOnDefaultLocalServer(int startInclusive, int endInclusive) {
        writeLogsOnServer(timeLockCorruptionTestSetup.getDefaultLocalServer(), startInclusive, endInclusive);
    }

    void writeLogsOnDefaultLocalAndRemote(int startInclusive, int endInclusive) {
        writeLogsOnLocalAndRemote(timeLockCorruptionTestSetup.getDefaultServerList(), startInclusive, endInclusive);
    }

    static void writeLogsOnLocalAndRemote(List<StateLogComponents> servers, int startingLogSeq, int latestLogSequence) {
        servers.forEach(server -> writeLogsOnServer(server, startingLogSeq, latestLogSequence));
    }

    StateLogComponents getDefaultLocalServer() {
        return timeLockCorruptionTestSetup.getDefaultLocalServer();
    }

    List<StateLogComponents> getDefaultRemoteServerList() {
        return timeLockCorruptionTestSetup.getDefaultRemoteServerList();
    }

    void induceGreaterAcceptedValueCorruptionOnDefaultLocalServer(int corruptSeq) {
        induceGreaterAcceptedValueCorruption(timeLockCorruptionTestSetup.getDefaultLocalServer(), corruptSeq);
    }

    static void induceGreaterAcceptedValueCorruption(StateLogComponents server, int corruptSeq) {
        PaxosSerializationTestUtils.writeAcceptorStateForLogAndRound(
                server.acceptorLog(),
                corruptSeq,
                Optional.of(PaxosSerializationTestUtils.createPaxosValueForRoundAndData(corruptSeq, corruptSeq + 1)));
    }

    List<CompletePaxosHistoryForNamespaceAndUseCase> getHistory() {
        return timeLockCorruptionTestSetup.getPaxosLogHistoryProvider().getHistory();
    }

    void assertNoCorruptionViolations() {
        assertThat(getViolationsToNamespaceToUseCaseMultimap().isEmpty()).isTrue();
    }

    void assertAcceptedValueGreaterThanLearnedValue() {
        assertViolationDetectedForNamespaceAndUseCases(
                CorruptionCheckViolation.ACCEPTED_VALUE_GREATER_THAN_LEARNED,
                ImmutableSet.of(TimelockCorruptionTestConstants.DEFAULT_NAMESPACE_AND_USE_CASE));
    }

    void assertViolationDetected(CorruptionCheckViolation expectedViolation) {
        assertViolationDetectedForNamespaceAndUseCases(
                expectedViolation, ImmutableSet.of(TimelockCorruptionTestConstants.DEFAULT_NAMESPACE_AND_USE_CASE));
    }

    void assertViolationDetectedForNamespaceAndUseCases(
            CorruptionCheckViolation expectedViolation,
            Set<NamespaceAndUseCase> expectedNamespaceAndUseCasesWithViolation) {
        Multimap<CorruptionCheckViolation, NamespaceAndUseCase> violationsToNamespaceToUseCaseMultimap =
                getViolationsToNamespaceToUseCaseMultimap();

        assertThat(violationsToNamespaceToUseCaseMultimap.keySet()).containsExactly(expectedViolation);
        assertThat(violationsToNamespaceToUseCaseMultimap.values())
                .hasSameElementsAs(expectedNamespaceAndUseCasesWithViolation);
    }

    List<StateLogComponents> createStatLogComponentsForNamespaceAndUseCase(NamespaceAndUseCase namespaceAndUseCase) {
        return timeLockCorruptionTestSetup.createStatLogForNamespaceAndUseCase(namespaceAndUseCase);
    }

    void assertClockGoesBackwardsInNextBatch() {
        assertLocalTimestampInvariants(ImmutableSet.of(CorruptionCheckViolation.CLOCK_WENT_BACKWARDS));
    }

    void assertLocalTimestampInvariantsStandInNextBatch() {
        assertLocalTimestampInvariants(ImmutableSet.of());
    }

    void createTimestampInversion(int round) {
        PaxosSerializationTestUtils.writePaxosValue(
                getDefaultLocalServer().learnerLog(),
                round,
                PaxosSerializationTestUtils.createPaxosValueForRoundAndData(round, round * 100));
    }

    private void assertLocalTimestampInvariants(Set<CorruptionCheckViolation> violations) {
        CorruptionHealthReport corruptionHealthReport = timeLockCorruptionTestSetup
                .getLocalTimestampInvariantsVerifier()
                .timestampInvariantsHealthReport();
        assertThat(corruptionHealthReport
                        .violatingStatusesToNamespaceAndUseCase()
                        .keySet())
                .hasSameElementsAs(violations);
    }

    private static void writeLogsOnServer(StateLogComponents server, int startInclusive, int endInclusive) {
        PaxosSerializationTestUtils.writeToLogs(
                server.acceptorLog(), server.learnerLog(), startInclusive, endInclusive);
    }

    private Multimap<CorruptionCheckViolation, NamespaceAndUseCase> getViolationsToNamespaceToUseCaseMultimap() {
        return HistoryAnalyzer.corruptionHealthReportForHistory(getHistory()).violatingStatusesToNamespaceAndUseCase();
    }
}
