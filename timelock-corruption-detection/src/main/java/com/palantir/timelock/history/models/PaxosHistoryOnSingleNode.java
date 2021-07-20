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

package com.palantir.timelock.history.models;

import static com.palantir.timelock.history.sqlite.LogDeletionMarker.INITIAL_DELETION_MARK;

import com.google.common.collect.ImmutableMap;
import com.palantir.paxos.NamespaceAndUseCase;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.immutables.value.Value;

@Value.Immutable
public interface PaxosHistoryOnSingleNode {
    @Value.Parameter
    Map<NamespaceAndUseCase, LearnerAndAcceptorRecords> history();

    default ConsolidatedLearnerAndAcceptorRecord getConsolidatedLocalAndRemoteRecord(
            NamespaceAndUseCase namespaceAndUseCase) {

        if (!history().containsKey(namespaceAndUseCase)) {
            return ConsolidatedLearnerAndAcceptorRecord.of(ImmutableMap.of(), Optional.of(INITIAL_DELETION_MARK));
        }

        LearnerAndAcceptorRecords records = history().get(namespaceAndUseCase);
        return ConsolidatedLearnerAndAcceptorRecord.of(
                consolidateRecordsForSequenceRange(records), Optional.of(records.greatestDeletedSeq()));
    }

    default Map<Long, LearnedAndAcceptedValue> consolidateRecordsForSequenceRange(LearnerAndAcceptorRecords records) {
        return records.getAllSequenceNumbers().stream()
                .collect(Collectors.toMap(Function.identity(), seq -> getLearnedAndAcceptedValues(records, seq)));
    }

    default LearnedAndAcceptedValue getLearnedAndAcceptedValues(LearnerAndAcceptorRecords records, Long seq) {
        return ImmutableLearnedAndAcceptedValue.of(
                records.getLearnedValueAtSeqIfExists(seq), records.getAcceptedValueAtSeqIfExists(seq));
    }
}
