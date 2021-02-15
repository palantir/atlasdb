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

import com.google.common.collect.Iterables;
import com.palantir.timelock.history.models.CompletePaxosHistoryForNamespaceAndUseCase;
import com.palantir.timelock.history.utils.PaxosSerializationTestUtils;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;

public final class HistoryAnalyzerTest {
    @Rule
    public TimeLockCorruptionDetectionHelper helper = new TimeLockCorruptionDetectionHelper();

    @Test
    public void correctlyPassesIfThereIsNotCorruption() {
        helper.writeLogsOnDefaultLocalAndRemote(67, 298);

        List<CompletePaxosHistoryForNamespaceAndUseCase> historyForAll = helper.getHistory();

        assertThat(HistoryAnalyzer.corruptionCheckViolationLevelForNamespaceAndUseCase(
                        Iterables.getOnlyElement(historyForAll)))
                .isEqualTo(CorruptionCheckViolation.NONE);

        helper.assertNoCorruptionViolations();
    }

    @Test
    public void detectCorruptionIfDifferentValuesAreLearnedInSameRound() {
        PaxosSerializationTestUtils.writePaxosValue(
                helper.getDefaultLocalServer().learnerLog(),
                1,
                PaxosSerializationTestUtils.createPaxosValueForRoundAndData(1, 1));
        helper.getDefaultRemoteServerList()
                .forEach(server -> PaxosSerializationTestUtils.writePaxosValue(
                        server.learnerLog(), 1, PaxosSerializationTestUtils.createPaxosValueForRoundAndData(1, 5)));
        List<CompletePaxosHistoryForNamespaceAndUseCase> historyForAll = helper.getHistory();
        assertThat(HistoryAnalyzer.divergedLearners(Iterables.getOnlyElement(historyForAll)))
                .isEqualTo(CorruptionCheckViolation.DIVERGED_LEARNERS);

        helper.assertViolationDetected(CorruptionCheckViolation.DIVERGED_LEARNERS);
    }

    @Test
    public void detectCorruptionIfLearnedValueIsNotAcceptedByQuorum() {
        helper.writeLogsOnDefaultLocalServer(5, MAX_ROWS_ALLOWED);

        List<CompletePaxosHistoryForNamespaceAndUseCase> historyForAll = helper.getHistory();
        assertThat(HistoryAnalyzer.divergedLearners(Iterables.getOnlyElement(historyForAll)))
                .isEqualTo(CorruptionCheckViolation.NONE);
        assertThat(HistoryAnalyzer.learnedValueWithoutQuorum(Iterables.getOnlyElement(historyForAll)))
                .isEqualTo(CorruptionCheckViolation.VALUE_LEARNED_WITHOUT_QUORUM);

        helper.assertViolationDetected(CorruptionCheckViolation.VALUE_LEARNED_WITHOUT_QUORUM);
    }

    @Test
    public void detectCorruptionIfLearnedValueIsNotTheGreatestAcceptedValue() {
        helper.writeLogsOnDefaultLocalAndRemote(9, MAX_ROWS_ALLOWED - 1);
        helper.induceGreaterAcceptedValueCorruptionOnDefaultLocalServer(MAX_ROWS_ALLOWED / 2);

        List<CompletePaxosHistoryForNamespaceAndUseCase> historyForAll = helper.getHistory();
        assertThat(HistoryAnalyzer.divergedLearners(Iterables.getOnlyElement(historyForAll)))
                .isEqualTo(CorruptionCheckViolation.NONE);
        assertThat(HistoryAnalyzer.learnedValueWithoutQuorum(Iterables.getOnlyElement(historyForAll)))
                .isEqualTo(CorruptionCheckViolation.NONE);
        assertThat(HistoryAnalyzer.greatestAcceptedValueNotLearned(Iterables.getOnlyElement(historyForAll)))
                .isEqualTo(CorruptionCheckViolation.ACCEPTED_VALUE_GREATER_THAN_LEARNED);

        helper.assertViolationDetected(CorruptionCheckViolation.ACCEPTED_VALUE_GREATER_THAN_LEARNED);
    }
}
