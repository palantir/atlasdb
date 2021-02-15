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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.Preconditions;
import com.palantir.paxos.ImmutableNamespaceAndUseCase;
import com.palantir.paxos.NamespaceAndUseCase;
import com.palantir.paxos.PaxosValue;
import com.palantir.timelock.history.PaxosAcceptorData;
import com.palantir.timelock.history.models.CompletePaxosHistoryForNamespaceAndUseCase;
import com.palantir.timelock.history.models.ConsolidatedLearnerAndAcceptorRecord;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class HistoryAnalyzer {
    private HistoryAnalyzer() {
        // do not create instance of this class
    }

    public static CorruptionHealthReport corruptionHealthReportForHistory(
            List<CompletePaxosHistoryForNamespaceAndUseCase> history) {

        Map<NamespaceAndUseCase, CorruptionCheckViolation> namespaceAndUseCaseCorruptionCheckViolationMap =
                history.stream()
                        .collect(Collectors.toMap(
                                HistoryAnalyzer::extractNamespaceAndUseCase,
                                HistoryAnalyzer::corruptionCheckViolationLevelForNamespaceAndUseCase));

        SetMultimap<CorruptionCheckViolation, NamespaceAndUseCase> namespacesExhibitingViolations = KeyedStream.stream(
                        namespaceAndUseCaseCorruptionCheckViolationMap)
                .mapEntries((k, v) -> Maps.immutableEntry(v, k))
                .filterKeys(CorruptionCheckViolation::raiseErrorAlert)
                .collectToSetMultimap();

        return ImmutableCorruptionHealthReport.builder()
                .violatingStatusesToNamespaceAndUseCase(namespacesExhibitingViolations)
                .build();
    }

    private static NamespaceAndUseCase extractNamespaceAndUseCase(
            CompletePaxosHistoryForNamespaceAndUseCase historyForNamespaceAndUseCase) {
        return ImmutableNamespaceAndUseCase.builder()
                .namespace(historyForNamespaceAndUseCase.namespace())
                .useCase(historyForNamespaceAndUseCase.useCase())
                .build();
    }

    @VisibleForTesting
    static CorruptionCheckViolation corruptionCheckViolationLevelForNamespaceAndUseCase(
            CompletePaxosHistoryForNamespaceAndUseCase history) {
        List<Function<CompletePaxosHistoryForNamespaceAndUseCase, CorruptionCheckViolation>> violationChecks =
                ImmutableList.of(
                        HistoryAnalyzer::divergedLearners,
                        HistoryAnalyzer::learnedValueWithoutQuorum,
                        HistoryAnalyzer::greatestAcceptedValueNotLearned);
        return violationChecks.stream()
                .map(check -> check.apply(history))
                .filter(CorruptionCheckViolation::raiseErrorAlert)
                .findFirst()
                .orElse(CorruptionCheckViolation.NONE);
    }

    @VisibleForTesting
    static CorruptionCheckViolation divergedLearners(CompletePaxosHistoryForNamespaceAndUseCase history) {
        List<ConsolidatedLearnerAndAcceptorRecord> records = history.localAndRemoteLearnerAndAcceptorRecords();
        return history.getAllSequenceNumbers().stream().allMatch(seq -> {
                    Set<PaxosValue> learnedValuesForRound = getLearnedValuesForRound(records, seq);
                    return learnedValuesForRound.size() <= 1;
                })
                ? CorruptionCheckViolation.NONE
                : CorruptionCheckViolation.DIVERGED_LEARNERS;
    }

    @VisibleForTesting
    static CorruptionCheckViolation learnedValueWithoutQuorum(CompletePaxosHistoryForNamespaceAndUseCase history) {
        List<ConsolidatedLearnerAndAcceptorRecord> records = history.localAndRemoteLearnerAndAcceptorRecords();
        int quorum = getQuorumSize(records);

        return history.getAllSequenceNumbers().stream()
                        .allMatch(seq -> isLearnedValueAcceptedByQuorum(records, quorum, seq))
                ? CorruptionCheckViolation.NONE
                : CorruptionCheckViolation.VALUE_LEARNED_WITHOUT_QUORUM;
    }

    @VisibleForTesting
    static CorruptionCheckViolation greatestAcceptedValueNotLearned(
            CompletePaxosHistoryForNamespaceAndUseCase history) {
        List<ConsolidatedLearnerAndAcceptorRecord> records = history.localAndRemoteLearnerAndAcceptorRecords();
        return history.getAllSequenceNumbers().stream()
                        .allMatch(seq -> learnedValueIsGreatestAcceptedValue(records, seq))
                ? CorruptionCheckViolation.NONE
                : CorruptionCheckViolation.ACCEPTED_VALUE_GREATER_THAN_LEARNED;
    }

    private static boolean isLearnedValueAcceptedByQuorum(
            List<ConsolidatedLearnerAndAcceptorRecord> records, int quorum, Long seq) {
        Optional<PaxosValue> optionalLearnedValue = getLearnedValue(records, seq);
        if (!optionalLearnedValue.isPresent()) {
            return true;
        }

        PaxosValue learnedValue = optionalLearnedValue.get();
        List<PaxosValue> acceptedValues = getAcceptedValues(records, seq, learnedValue);
        return acceptedValues.size() >= quorum;
    }

    private static boolean learnedValueIsGreatestAcceptedValue(
            List<ConsolidatedLearnerAndAcceptorRecord> records, Long seq) {
        byte[] learnedValueData = getPaxosValueData(getLearnedValue(records, seq));
        if (learnedValueData == null) {
            return true;
        }

        byte[] greatestAcceptedValueData = getPaxosValueData(getGreatestAcceptedValueAtSequence(records, seq));

        Preconditions.checkNotNull(
                greatestAcceptedValueData,
                "Value learned did have data while there "
                        + "was no data in any of the accepted states. This should never happen, contact support");
        return PtBytes.toLong(greatestAcceptedValueData) <= PtBytes.toLong(learnedValueData);
    }

    private static Optional<PaxosValue> getLearnedValue(
            List<ConsolidatedLearnerAndAcceptorRecord> recordList, Long seq) {
        Set<PaxosValue> values = getLearnedValuesForRound(recordList, seq);
        return values.isEmpty() ? Optional.empty() : Optional.of(Iterables.getOnlyElement(values));
    }

    private static Set<PaxosValue> getLearnedValuesForRound(
            List<ConsolidatedLearnerAndAcceptorRecord> recordList, Long seq) {
        return recordList.stream()
                .map(consolidatedLearnerAndAcceptorRecord ->
                        consolidatedLearnerAndAcceptorRecord.get(seq).learnedValue())
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toSet());
    }

    private static int getQuorumSize(List<ConsolidatedLearnerAndAcceptorRecord> records) {
        return records.size() / 2 + 1;
    }

    private static List<PaxosValue> getAcceptedValues(
            List<ConsolidatedLearnerAndAcceptorRecord> records, Long seq, PaxosValue learnedValue) {
        return records.stream()
                .map(record -> record.get(seq)
                        .acceptedValue()
                        .map(PaxosAcceptorData::getLastAcceptedValue)
                        .orElseGet(Optional::empty))
                .filter(optionalPaxosValue -> optionalPaxosValue.isPresent()
                        && optionalPaxosValue.get().equals(learnedValue))
                .map(Optional::get)
                .collect(Collectors.toList());
    }

    private static Optional<PaxosValue> getGreatestAcceptedValueAtSequence(
            List<ConsolidatedLearnerAndAcceptorRecord> records, long seq) {
        return records.stream()
                .map(record -> record.get(seq)
                        .acceptedValue()
                        .map(PaxosAcceptorData::getLastAcceptedValue)
                        .orElseGet(Optional::empty))
                .filter(paxosValue -> getPaxosValueData(paxosValue) != null)
                .map(Optional::get)
                .max(Comparator.comparingLong(paxosValue -> PtBytes.toLong(paxosValue.getData())));
    }

    // This method is only used for paxos rounds run for timestamp consensus (leader paxos rounds are strictly ignored).
    private static byte[] getPaxosValueData(Optional<PaxosValue> learnedValue) {
        return learnedValue.map(PaxosValue::getData).orElse(null);
    }
}
