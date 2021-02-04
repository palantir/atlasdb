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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.palantir.timelock.corruption.detection.HistoryAnalyzer.CorruptedSeqNumbers;
import com.palantir.timelock.history.models.CompletePaxosHistoryForNamespaceAndUseCase;
import com.palantir.timelock.history.utils.PaxosSerializationTestUtils;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
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
        CompletePaxosHistoryForNamespaceAndUseCase history = Iterables.getOnlyElement(historyForAll);
        assertThat(HistoryAnalyzer.divergedLearners(history, history.getAllSequenceNumbers()))
                .isEqualTo(CorruptedSeqNumbers.of(CorruptionCheckViolation.DIVERGED_LEARNERS, ImmutableSet.of(1L)));

        helper.assertViolationDetected(CorruptionCheckViolation.DIVERGED_LEARNERS);
    }

    @Test
    public void detectCorruptionIfLearnedValueIsNotAcceptedByQuorum() {
        helper.writeLogsOnDefaultLocalServer(5, MAX_ROWS_ALLOWED);

        List<CompletePaxosHistoryForNamespaceAndUseCase> historyForAll = helper.getHistory();
        CompletePaxosHistoryForNamespaceAndUseCase history = Iterables.getOnlyElement(historyForAll);
        Set<Long> allSequenceNumbers = history.getAllSequenceNumbers();
        assertThat(HistoryAnalyzer.divergedLearners(history, allSequenceNumbers))
                .isEqualTo(CorruptedSeqNumbers.of(CorruptionCheckViolation.NONE, ImmutableSet.of()));
        assertThat(HistoryAnalyzer.learnedValueWithoutQuorum(history, allSequenceNumbers))
                .isEqualTo(CorruptedSeqNumbers.of(
                        CorruptionCheckViolation.VALUE_LEARNED_WITHOUT_QUORUM,
                        LongStream.range(5, MAX_ROWS_ALLOWED).boxed().collect(Collectors.toSet())));

        helper.assertViolationDetected(CorruptionCheckViolation.VALUE_LEARNED_WITHOUT_QUORUM);
    }

    @Test
    public void returnsRightLevelOfViolationForMultipleViolationsAcrossSequences() {
        PaxosSerializationTestUtils.writePaxosValue(
                helper.getDefaultLocalServer().learnerLog(),
                1,
                PaxosSerializationTestUtils.createPaxosValueForRoundAndData(1, 1));
        helper.getDefaultRemoteServerList()
                .forEach(server -> PaxosSerializationTestUtils.writePaxosValue(
                        server.learnerLog(), 1, PaxosSerializationTestUtils.createPaxosValueForRoundAndData(1, 5)));

        helper.writeLogsOnDefaultLocalServer(5, MAX_ROWS_ALLOWED - 1);

        helper.assertViolationDetected(CorruptionCheckViolation.DIVERGED_LEARNERS);
    }

    @Test
    public void detectCorruptionIfLearnedValueIsNotTheGreatestAcceptedValue() {
        helper.writeLogsOnDefaultLocalAndRemote(9, MAX_ROWS_ALLOWED - 1);
        helper.induceGreaterAcceptedValueCorruptionOnDefaultLocalServer(MAX_ROWS_ALLOWED / 2);

        List<CompletePaxosHistoryForNamespaceAndUseCase> historyForAll = helper.getHistory();
        CompletePaxosHistoryForNamespaceAndUseCase history = Iterables.getOnlyElement(historyForAll);
        Set<Long> allSequenceNumbers = history.getAllSequenceNumbers();
        assertThat(HistoryAnalyzer.divergedLearners(history, allSequenceNumbers))
                .isEqualTo(CorruptedSeqNumbers.of(CorruptionCheckViolation.NONE, ImmutableSet.of()));
        assertThat(HistoryAnalyzer.learnedValueWithoutQuorum(history, allSequenceNumbers))
                .isEqualTo(CorruptedSeqNumbers.of(CorruptionCheckViolation.NONE, ImmutableSet.of()));
        assertThat(HistoryAnalyzer.greatestAcceptedValueNotLearned(history, allSequenceNumbers))
                .isEqualTo(CorruptedSeqNumbers.of(
                        CorruptionCheckViolation.ACCEPTED_VALUE_GREATER_THAN_LEARNED,
                        ImmutableSet.of(MAX_ROWS_ALLOWED / 2L)));

        helper.assertViolationDetected(CorruptionCheckViolation.ACCEPTED_VALUE_GREATER_THAN_LEARNED);
    }
}
