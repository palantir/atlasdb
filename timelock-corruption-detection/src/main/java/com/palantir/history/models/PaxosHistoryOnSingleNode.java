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

package com.palantir.history.models;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.immutables.value.Value;

import com.google.common.collect.ImmutableMap;
import com.palantir.paxos.NamespaceAndUseCase;

@Value.Immutable
public interface PaxosHistoryOnSingleNode {

    @Value.Parameter
    Map<NamespaceAndUseCase, LearnerAndAcceptorRecords> history();

    default ConsolidatedLearnerAndAcceptorRecord getConsolidatedLocalAndRemoteRecord(
            NamespaceAndUseCase namespaceAndUseCase) {

        if (!history().containsKey(namespaceAndUseCase)) {
            return ImmutableConsolidatedLearnerAndAcceptorRecord.of(ImmutableMap.of());
        }
        LearnerAndAcceptorRecords records = history().get(namespaceAndUseCase);

        long minSeq = records.getMinSequence();
        long maxSeq = records.getMaxSequence();

        return ImmutableConsolidatedLearnerAndAcceptorRecord.of(
                consolidateRecordsForSequenceRange(records, minSeq, maxSeq));
    }

    default Map<Long, LearnedAndAcceptedValue> consolidateRecordsForSequenceRange(LearnerAndAcceptorRecords records,
            long minSeq, long maxSeq) {
        return LongStream.rangeClosed(minSeq, maxSeq).boxed().collect(
                Collectors.toMap(Function.identity(), seq -> getLearnedAndAcceptedValues(records, seq)));
    }

    default LearnedAndAcceptedValue getLearnedAndAcceptedValues(LearnerAndAcceptorRecords records, Long seq) {
        return ImmutableLearnedAndAcceptedValue.of(records.getLearnedValueAtSeqIfExists(seq),
                records.getAcceptedValueAtSeqIfExists(seq));
    }
}
