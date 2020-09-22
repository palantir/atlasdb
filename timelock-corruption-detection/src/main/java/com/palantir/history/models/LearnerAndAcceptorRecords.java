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

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import org.immutables.value.Value;

import com.palantir.paxos.PaxosAcceptorState;
import com.palantir.paxos.PaxosValue;

@Value.Immutable
public interface LearnerAndAcceptorRecords {

    @Value.Parameter
    Map<Long, PaxosValue> learnerRecords();

    @Value.Parameter
    Map<Long, PaxosAcceptorState> acceptorRecords();

    default Optional<PaxosValue> getLearnedValueAtSeqIfExists(long seq) {
        return learnerRecords().containsKey(seq) ? Optional.of(learnerRecords().get(seq)) : Optional.empty();
    }

    default Optional<PaxosAcceptorState> getAcceptedValueAtSeqIfExists(long seq) {
        return learnerRecords().containsKey(seq) ? Optional.of(acceptorRecords().get(seq)) : Optional.empty();
    }

    default long getMinSequence() {
        long minSeq = Long.MAX_VALUE;
        if (!learnerRecords().isEmpty()) {
            minSeq = Math.min(minSeq, Collections.min(learnerRecords().keySet()));
        }
        if (!acceptorRecords().isEmpty()) {
            minSeq = Math.min(minSeq, Collections.min(acceptorRecords().keySet()));
        }
        return minSeq;
    }

    default long getMaxSequence() {
        long maxSeq = Long.MIN_VALUE;
        if (!learnerRecords().isEmpty()) {
            maxSeq = Math.max(maxSeq, Collections.max(learnerRecords().keySet()));
        }
        if (!acceptorRecords().isEmpty()) {
            maxSeq = Math.max(maxSeq, Collections.max(acceptorRecords().keySet()));
        }
        return maxSeq;
    }

    default long getMinSequence() {
        long minSeq = Long.MAX_VALUE;
        if (!learnerRecords().isEmpty()) {
            minSeq = Math.min(minSeq, Collections.min(learnerRecords().keySet()));
        }
        if (!acceptorRecords().isEmpty()) {
            minSeq = Math.min(minSeq, Collections.min(acceptorRecords().keySet()));
        }
        return minSeq;
    }

    default long getMaxSequence() {
        long maxSeq = Long.MIN_VALUE;
        if (!learnerRecords().isEmpty()) {
            maxSeq = Math.max(maxSeq, Collections.max(learnerRecords().keySet()));
        }
        if (!acceptorRecords().isEmpty()) {
            maxSeq = Math.max(maxSeq, Collections.max(acceptorRecords().keySet()));
        }
        return maxSeq;
    }
}
