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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.paxos.PaxosValue;
import com.palantir.timelock.history.PaxosAcceptorData;
import com.palantir.timelock.history.models.CompletePaxosHistoryForNamespaceAndUseCase;
import com.palantir.timelock.history.models.ConsolidatedLearnerAndAcceptorRecord;

public final class HistoryAnalyzer {

    public static boolean runCorruptionCheckOnHistory(CompletePaxosHistoryForNamespaceAndUseCase history) {
        return verifyLearnersHaveLearnedSameValues(history)
        && verifyLearnedValueWasAcceptedByQuorum(history)
        && verifyLearnedValueIsGreatestAcceptedValue(history);
    }

    @VisibleForTesting
    static boolean verifyLearnersHaveLearnedSameValues(CompletePaxosHistoryForNamespaceAndUseCase history) {
        Set<Long> allSequenceNumbers = history.getAllSequenceNumbers();
        List<ConsolidatedLearnerAndAcceptorRecord> records = history.localAndRemoteLearnerAndAcceptorRecords();
        return allSequenceNumbers.stream().allMatch(seq -> {
            Set<PaxosValue> learnedValuesForRound = getLearnedValuesForRound(records, seq);
            return learnedValuesForRound.size() <= 1;
        });
    }

    @VisibleForTesting
    static boolean verifyLearnedValueWasAcceptedByQuorum(CompletePaxosHistoryForNamespaceAndUseCase history) {
        List<ConsolidatedLearnerAndAcceptorRecord> records = history.localAndRemoteLearnerAndAcceptorRecords();
        int quorum = getQuorumSize(records);

        return history.getAllSequenceNumbers().stream().allMatch(seq -> {
            Optional<PaxosValue> optionalLearnedValue = getLearnedValue(records, seq);

            if(!optionalLearnedValue.isPresent()) {
                return true;
            }

            PaxosValue learnedValue = optionalLearnedValue.get();

            List<PaxosValue> acceptedValues = getAcceptedValues(records, seq, learnedValue);
            return acceptedValues.size() >= quorum;
        });
    }

    @VisibleForTesting
    static boolean verifyLearnedValueIsGreatestAcceptedValue(CompletePaxosHistoryForNamespaceAndUseCase history) {
        Set<Long> allSequenceNumbers = history.getAllSequenceNumbers();
        List<ConsolidatedLearnerAndAcceptorRecord> records = history.localAndRemoteLearnerAndAcceptorRecords();
        return allSequenceNumbers.stream().allMatch(seq -> learnedValueIsGreatestAcceptedValue(records, seq));
    }

    // utils
    private static boolean learnedValueIsGreatestAcceptedValue(
            List<ConsolidatedLearnerAndAcceptorRecord> records, Long seq) {

        Optional<PaxosValue> optionalLearnedValue = getLearnedValue(records, seq);
        if(!optionalLearnedValue.isPresent()) {
            return true;
        }

        PaxosValue learnedValue = optionalLearnedValue.get();
        if (learnedValue.getData() == null) {
            return true;
        }

        Optional<PaxosValue> greatestAcceptedValue = getGreatestAcceptedValueAtSequence(records, seq);
        if (!greatestAcceptedValue.isPresent()) {
            // should not reach here
            return false;
        }
        return PtBytes.toLong(greatestAcceptedValue.get().getData()) <= PtBytes.toLong(learnedValue.getData());
    }

    private static Optional<PaxosValue> getLearnedValue(List<ConsolidatedLearnerAndAcceptorRecord> recordList, Long seq) {
        Set<PaxosValue> values = getLearnedValuesForRound(recordList, seq);
        return values.isEmpty() ? Optional.empty() : Optional.of(Iterables.getOnlyElement(values));
    }

    private static Set<PaxosValue> getLearnedValuesForRound(List<ConsolidatedLearnerAndAcceptorRecord> recordList,
            Long seq) {
        return recordList
                .stream()
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
        return records
                .stream()
                .map(record -> record.get(seq).acceptedValue())
                .filter(Optional::isPresent)
                .map(acceptorData -> acceptorData.get().getLastAcceptedValue())
                .filter(Optional::isPresent)
                .map(Optional::get)
                .filter(value -> value.equals(learnedValue))
                .collect(Collectors.toList());
    }

    private static Optional<PaxosValue> getGreatestAcceptedValueAtSequence(
            List<ConsolidatedLearnerAndAcceptorRecord> records, long seq) {
        return records.stream()
                .map(record -> record.get(seq).acceptedValue())
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(PaxosAcceptorData::getLastAcceptedValue)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .filter(paxosValue -> paxosValue.getData() != null)
                .max(Comparator.comparingLong(paxosValue ->  PtBytes.toLong(paxosValue.getData())));
    }
}
