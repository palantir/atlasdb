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

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
        return Optional.ofNullable(learnerRecords().get(seq));
    }

    default Optional<PaxosAcceptorState> getAcceptedValueAtSeqIfExists(long seq) {
        return Optional.ofNullable(acceptorRecords().get(seq));
    }

    // it is okay to have a set of longs as learner and acceptor records have a limit of 500 entries.
    @Value.Lazy
    default Set<Long> getAllSequenceNumbers() {
        return Stream.of(learnerRecords(), acceptorRecords())
                .map(Map::keySet)
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
    }
}
