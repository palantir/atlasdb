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


import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.SetMultimap;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.paxos.ImmutableNamespaceAndUseCase;
import com.palantir.paxos.NamespaceAndUseCase;
import com.palantir.paxos.PaxosValue;
import com.palantir.timelock.history.PaxosAcceptorData;
import com.palantir.timelock.history.models.CompletePaxosHistoryForNamespaceAndUseCase;
import com.palantir.timelock.history.models.ConsolidatedLearnerAndAcceptorRecord;

public final class HistoryAnalyzer {
    public static CorruptionHealthReport corruptionStateForHistory(
            List<CompletePaxosHistoryForNamespaceAndUseCase> history) {
        SetMultimap<CorruptionStatus, NamespaceAndUseCase> statusNamespaceAndUseCase = LinkedHashMultimap.create();
        for (CompletePaxosHistoryForNamespaceAndUseCase historyForNamespaceAndUseCase: history) {
            for (CorruptionStatus status: corruptionStateForNamespaceAndUseCase(historyForNamespaceAndUseCase)) {
                ImmutableNamespaceAndUseCase namespaceAndUseCase = ImmutableNamespaceAndUseCase.builder().namespace(
                        historyForNamespaceAndUseCase.namespace()).useCase(
                        historyForNamespaceAndUseCase.useCase()).build();
                statusNamespaceAndUseCase.put(status, namespaceAndUseCase);
            }
        }
        return ImmutableCorruptionHealthReport.builder().statusesToNamespaceAndUseCase(statusNamespaceAndUseCase).build();
    }

    public static List<CorruptionStatus> corruptionStateForNamespaceAndUseCase(
            CompletePaxosHistoryForNamespaceAndUseCase history) {
        return Stream.of(learnersHaveLearnedSameValues(history),
                learnedValueWasAcceptedByQuorum(history),
                learnedValueIsGreatestAcceptedValue(history))
                .filter(status -> status != CorruptionStatus.HEALTHY)
                .collect(Collectors.toList());
    }

    @VisibleForTesting
    static CorruptionStatus learnersHaveLearnedSameValues(CompletePaxosHistoryForNamespaceAndUseCase history) {
        List<ConsolidatedLearnerAndAcceptorRecord> records = history.localAndRemoteLearnerAndAcceptorRecords();
        return history.getAllSequenceNumbers()
                .stream()
                .allMatch(seq -> {
                    Set<PaxosValue> learnedValuesForRound = getLearnedValuesForRound(records, seq);
                    return learnedValuesForRound.size() <= 1;
                }) ? CorruptionStatus.HEALTHY : CorruptionStatus.DIVERGED_LEARNERS;
    }

    @VisibleForTesting
    static CorruptionStatus learnedValueWasAcceptedByQuorum(CompletePaxosHistoryForNamespaceAndUseCase history) {
        List<ConsolidatedLearnerAndAcceptorRecord> records = history.localAndRemoteLearnerAndAcceptorRecords();
        int quorum = getQuorumSize(records);

        return history.getAllSequenceNumbers()
                .stream()
                .allMatch(seq -> {
                    Optional<PaxosValue> optionalLearnedValue = getLearnedValue(records, seq);
                    if(!optionalLearnedValue.isPresent()) {
                        return true;
                    }

                    PaxosValue learnedValue = optionalLearnedValue.get();
                    List<PaxosValue> acceptedValues = getAcceptedValues(records, seq, learnedValue);
                    return acceptedValues.size() >= quorum;
                }) ? CorruptionStatus.HEALTHY : CorruptionStatus.VALUE_LEARNED_WITHOUT_QUORUM;
    }

    @VisibleForTesting
    static CorruptionStatus learnedValueIsGreatestAcceptedValue(CompletePaxosHistoryForNamespaceAndUseCase history) {
        List<ConsolidatedLearnerAndAcceptorRecord> records = history.localAndRemoteLearnerAndAcceptorRecords();
        return history.getAllSequenceNumbers()
                .stream()
                .allMatch(seq -> learnedValueIsGreatestAcceptedValue(records, seq))
                ? CorruptionStatus.HEALTHY : CorruptionStatus.DIVERGED_LEARNERS;
    }

    private static boolean learnedValueIsGreatestAcceptedValue(
            List<ConsolidatedLearnerAndAcceptorRecord> records, Long seq) {
        byte[] learnedValueData = getPaxosValueData(getLearnedValue(records, seq));
        if (learnedValueData == null) {
            return true;
        }

        byte[] greatestAcceptedValueData = getPaxosValueData(getGreatestAcceptedValueAtSequence(records, seq));
        if (greatestAcceptedValueData == null) {
            // should not reach here
            return false;
        }
        return PtBytes.toLong(greatestAcceptedValueData) <= PtBytes.toLong(learnedValueData);
    }

    private static Optional<PaxosValue> getLearnedValue(List<ConsolidatedLearnerAndAcceptorRecord> recordList, Long seq) {
        Set<PaxosValue> values = getLearnedValuesForRound(recordList, seq);
        return values.isEmpty() ? Optional.empty() : Optional.of(Iterables.getOnlyElement(values));
    }

    private static Set<PaxosValue> getLearnedValuesForRound(List<ConsolidatedLearnerAndAcceptorRecord> recordList,
            Long seq) {
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

    private static List<PaxosValue> getAcceptedValues(List<ConsolidatedLearnerAndAcceptorRecord> records, Long seq,
            PaxosValue learnedValue) {
        return records.stream()
                .map(record -> record.get(seq)
                        .acceptedValue()
                        .map(PaxosAcceptorData::getLastAcceptedValue)
                        .orElseGet(Optional::empty))
                .filter(optionalPaxosValue ->
                        optionalPaxosValue.isPresent() && optionalPaxosValue.get().equals(learnedValue))
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
                .max(Comparator.comparingLong(paxosValue ->  PtBytes.toLong(paxosValue.getData())));
    }

    private static byte[] getPaxosValueData(Optional<PaxosValue> learnedValue) {
        return learnedValue.map(PaxosValue::getData).orElse(null);
    }
}
