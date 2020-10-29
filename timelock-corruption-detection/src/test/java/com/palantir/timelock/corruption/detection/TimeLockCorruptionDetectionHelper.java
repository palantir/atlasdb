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
import com.palantir.paxos.NamespaceAndUseCase;
import com.palantir.paxos.PaxosValue;
import com.palantir.timelock.Constants;
import com.palantir.timelock.corruption.detection.TimeLockCorruptionTestSetup.StateLogComponents;
import com.palantir.timelock.history.models.CompletePaxosHistoryForNamespaceAndUseCase;
import com.palantir.timelock.history.utils.PaxosSerializationTestUtils;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class TimeLockCorruptionDetectionHelper implements TestRule {
    private TimeLockCorruptionTestSetup timeLockCorruptionTestSetup = new TimeLockCorruptionTestSetup();

    Set<PaxosValue> writeLogsOnDefaultLocalServer(int start, int end) {
        return writeLogsOnServer(timeLockCorruptionTestSetup.getDefaultLocalServer(), start, end);
    }

    Set<PaxosValue> writeLogsOnServer(StateLogComponents server, int start, int end) {
        return PaxosSerializationTestUtils.writeToLogs(server.acceptorLog(), server.learnerLog(), start, end);
    }

    void induceGreaterAcceptedValueCorruptionOnDefaultLocalServer(int corruptSeq) {
        induceGreaterAcceptedValueCorruption(timeLockCorruptionTestSetup.getDefaultLocalServer(), corruptSeq);
    }

    void induceGreaterAcceptedValueCorruption(StateLogComponents server, int corruptSeq) {
        PaxosSerializationTestUtils.wqqqq:riteAcceptorStateForLogAndRound(
                server.acceptorLog(),
                corruptSeq,
                Optional.of(PaxosSerializationTestUtils.createPaxosValueForRoundAndData(corruptSeq, corruptSeq + 1)));
    }

    void writeLogsOnDefaultLocalAndRemote(int startingLogSeq, int latestLogSequence) {
        writeLogsOnLocalAndRemote(
                timeLockCorruptionTestSetup.getDefaultServerList(), startingLogSeq, latestLogSequence);
    }

    void writeLogsOnLocalAndRemote(List<StateLogComponents> servers, int startingLogSeq, int latestLogSequence) {
        servers.stream().forEach(server -> writeLogsOnServer(server, startingLogSeq, latestLogSequence));
    }

    SetMultimap<CorruptionCheckViolation, NamespaceAndUseCase> getViolationsToNamespaceToUseCaseMultimap() {
        List<CompletePaxosHistoryForNamespaceAndUseCase> historyForAll = getHistory();
        return HistoryAnalyzer.corruptionHealthReportForHistory(historyForAll).violatingStatusesToNamespaceAndUseCase();
    }

    List<CompletePaxosHistoryForNamespaceAndUseCase> getHistory() {
        return timeLockCorruptionTestSetup.getPaxosLogHistoryProvider().getHistory();
    }

    void assertDetectedViolations(Set<CorruptionCheckViolation> detectedViolations) {
        assertDetectedViolations(detectedViolations, ImmutableSet.of(Constants.DEFAULT_NAMESPACE_AND_USE_CASE));
    }

    List<StateLogComponents> getDefaultRemoteServerList() {
        return timeLockCorruptionTestSetup.getDefaultRemoteServerList();
    }

    StateLogComponents getDefaultLocalServer() {
        return timeLockCorruptionTestSetup.getDefaultLocalServer();
    }

    void assertDetectedViolations(
            Set<CorruptionCheckViolation> detectedViolations,
            Set<NamespaceAndUseCase> namespaceAndUseCasesWithViolation) {
        SetMultimap<CorruptionCheckViolation, NamespaceAndUseCase> violationsToNamespaceToUseCaseMultimap =
                getViolationsToNamespaceToUseCaseMultimap();
        assertThat(violationsToNamespaceToUseCaseMultimap.keySet()).hasSameElementsAs(detectedViolations);
        assertThat(violationsToNamespaceToUseCaseMultimap.values())
                .hasSameElementsAs(namespaceAndUseCasesWithViolation);
    }

    public List<StateLogComponents> createStatLogComponentsForNamespaceAndUseCase(
            NamespaceAndUseCase namespaceAndUseCase) {
        return timeLockCorruptionTestSetup.createStatLogComponentsForNamespaceAndUseCase(namespaceAndUseCase);
    }

    @Override
    public Statement apply(Statement base, Description description) {
        return timeLockCorruptionTestSetup.apply(base, description);
    }
}
