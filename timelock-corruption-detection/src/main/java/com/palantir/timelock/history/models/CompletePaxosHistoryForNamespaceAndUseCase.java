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

import com.palantir.paxos.Client;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.immutables.value.Value;

/**
 * Data structure to contain Paxos learner and acceptor state values against sequence numbers
 * across all nodes for ({@link Client}, useCase) pair.
 *
 * Note - The useCase string here only contains the prefix
 * e.g. clientPaxos as opposed to clientPaxos!learner / clientPaxos!acceptor
 */
@Value.Immutable
public interface CompletePaxosHistoryForNamespaceAndUseCase {
    @Value.Parameter
    Client namespace();

    @Value.Parameter
    String useCase();

    @Value.Parameter
    List<ConsolidatedLearnerAndAcceptorRecord> localAndRemoteLearnerAndAcceptorRecords();

    @Value.Lazy
    default Set<Long> getAllSequenceNumbers() {
        return localAndRemoteLearnerAndAcceptorRecords().stream()
                .map(ConsolidatedLearnerAndAcceptorRecord::record)
                .map(Map::keySet)
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
    }
}
