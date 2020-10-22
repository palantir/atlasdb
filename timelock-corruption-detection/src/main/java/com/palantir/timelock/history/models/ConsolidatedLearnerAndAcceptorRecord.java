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

import static com.palantir.timelock.history.models.LearnedAndAcceptedValue.voidLearnedAndAcceptedValue;

import java.util.Map;
import org.immutables.value.Value;

/**
 * Rather than having two maps - one for learner records and one for acceptor records,
 * the consolidated record has a sequence number mapped to pair of (learnedValue, acceptedValue).
 * The data is consolidated this way because most of our assertions are around Paxos values at the same
 * seq number instead of values across multiple sequence numbers.
 */
@Value.Immutable
public interface ConsolidatedLearnerAndAcceptorRecord {

    @Value.Parameter
    Map<Long, LearnedAndAcceptedValue> record();

    static ConsolidatedLearnerAndAcceptorRecord of(Map<Long, LearnedAndAcceptedValue> record) {
        return ImmutableConsolidatedLearnerAndAcceptorRecord.of(record);
    }

    default LearnedAndAcceptedValue get(long seq) {
        return record().getOrDefault(seq, voidLearnedAndAcceptedValue());
    }
}
