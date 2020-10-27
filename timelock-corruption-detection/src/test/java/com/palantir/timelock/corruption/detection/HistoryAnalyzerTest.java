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

import com.google.common.collect.Iterables;
import com.google.common.collect.SetMultimap;
import com.palantir.paxos.NamespaceAndUseCase;
import com.palantir.timelock.history.models.CompletePaxosHistoryForNamespaceAndUseCase;
import com.palantir.timelock.history.utils.PaxosSerializationTestUtils;
import java.util.List;
import org.junit.Test;

public class HistoryAnalyzerTest extends CorruptionDetectionEteSetup {

    @Test
    public void correctlyPassesIfThereIsNotCorruption() {
        writeLogsOnServer(localStateLogComponents, 1, 10);
        remoteStateLogComponents.forEach(server -> writeLogsOnServer(server, 1, 10));

        List<CompletePaxosHistoryForNamespaceAndUseCase> historyForAll = paxosLogHistoryProvider.getHistory();

        assertThat(HistoryAnalyzer.corruptionCheckViolationLevelForNamespaceAndUseCase(
                        Iterables.getOnlyElement(historyForAll)))
                .isEqualTo(CorruptionCheckViolation.NONE);

        assertThat(HistoryAnalyzer.corruptionHealthReportForHistory(historyForAll)
                        .violatingStatusesToNamespaceAndUseCase()
                        .isEmpty())
                .isTrue();
    }

    @Test
    public void detectCorruptionIfDifferentValuesAreLearnedInSameRound() {
        PaxosSerializationTestUtils.writePaxosValue(
                localStateLogComponents.learnerLog(),
                1,
                PaxosSerializationTestUtils.createPaxosValueForRoundAndData(1, 1));
        remoteStateLogComponents.forEach(server -> PaxosSerializationTestUtils.writePaxosValue(
                server.learnerLog(), 1, PaxosSerializationTestUtils.createPaxosValueForRoundAndData(1, 5)));

        List<CompletePaxosHistoryForNamespaceAndUseCase> historyForAll = paxosLogHistoryProvider.getHistory();
        assertThat(HistoryAnalyzer.divergedLearners(Iterables.getOnlyElement(historyForAll)))
                .isEqualTo(CorruptionCheckViolation.DIVERGED_LEARNERS);

        SetMultimap<CorruptionCheckViolation, NamespaceAndUseCase> setMultimap =
                HistoryAnalyzer.corruptionHealthReportForHistory(historyForAll)
                        .violatingStatusesToNamespaceAndUseCase();
        assertThat(setMultimap.keySet()).containsExactly(CorruptionCheckViolation.DIVERGED_LEARNERS);
    }

    @Test
    public void detectCorruptionIfLearnedValueIsNotAcceptedByQuorum() {
        writeLogsOnServer(localStateLogComponents, 1, 10);

        List<CompletePaxosHistoryForNamespaceAndUseCase> historyForAll = paxosLogHistoryProvider.getHistory();
        assertThat(HistoryAnalyzer.divergedLearners(Iterables.getOnlyElement(historyForAll)))
                .isEqualTo(CorruptionCheckViolation.NONE);
        assertThat(HistoryAnalyzer.learnedValueWithoutQuorum(Iterables.getOnlyElement(historyForAll)))
                .isEqualTo(CorruptionCheckViolation.VALUE_LEARNED_WITHOUT_QUORUM);

        SetMultimap<CorruptionCheckViolation, NamespaceAndUseCase> setMultimap =
                HistoryAnalyzer.corruptionHealthReportForHistory(historyForAll)
                        .violatingStatusesToNamespaceAndUseCase();
        assertThat(setMultimap.keySet()).containsExactly(CorruptionCheckViolation.VALUE_LEARNED_WITHOUT_QUORUM);
    }

    @Test
    public void detectCorruptionIfLearnedValueIsNotTheGreatestAcceptedValue() {
        induceGreaterAcceptedValueCorruption(1, 5, 5);


        List<CompletePaxosHistoryForNamespaceAndUseCase> historyForAll = paxosLogHistoryProvider.getHistory();
        assertThat(HistoryAnalyzer.divergedLearners(Iterables.getOnlyElement(historyForAll)))
                .isEqualTo(CorruptionCheckViolation.NONE);
        assertThat(HistoryAnalyzer.learnedValueWithoutQuorum(Iterables.getOnlyElement(historyForAll)))
                .isEqualTo(CorruptionCheckViolation.NONE);
        assertThat(HistoryAnalyzer.greatestAcceptedValueNotLearned(Iterables.getOnlyElement(historyForAll)))
                .isEqualTo(CorruptionCheckViolation.ACCEPTED_VALUE_GREATER_THAN_LEARNED);

        SetMultimap<CorruptionCheckViolation, NamespaceAndUseCase> setMultimap =
                HistoryAnalyzer.corruptionHealthReportForHistory(historyForAll)
                        .violatingStatusesToNamespaceAndUseCase();
        assertThat(setMultimap.keySet()).containsExactly(CorruptionCheckViolation.ACCEPTED_VALUE_GREATER_THAN_LEARNED);
    }
}
