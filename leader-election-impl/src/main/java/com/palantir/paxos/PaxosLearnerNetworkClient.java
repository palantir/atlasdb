/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.paxos;

import java.util.Optional;
import java.util.function.Function;

/**
 * {@link PaxosLearnerNetworkClient} encapsulates the consensus portion of the request and involves communicating with
 * multiple {@link PaxosLearner}s. This should be used over {@link PaxosQuorumChecker} and {@link PaxosAcceptor} where
 * possible. This allows us to specifically tailor our approach for single leader vs multi leader configurations.
 */
public interface PaxosLearnerNetworkClient {

    /**
     * Teaches the given value for the seq-th round to all (read {@code quorumSize}) learner nodes including the local
     * node.
     *
     * This will attempt to communicate with all learner nodes in the cluster.
     *
     * @param seq round in question
     * @param value value learned for that round
     * @see PaxosLearner#learn
     */
    void learn(long seq, PaxosValue value);

    /**
     * Retrieves the learned value for the given seq from the cluster.
     * <p>
     * Calls {@code mapper} with the learned value or {@code Optional.empty()} if the value at {@code seq} has not been
     * learned. This will attempt to communicate with all learner nodes in the cluster and should collect at least
     * {@code quorumResponses}.
     *
     * @see PaxosLearner#getLearnedValue
     */
    <T extends PaxosResponse> PaxosResponses<T> getLearnedValue(long seq, Function<Optional<PaxosValue>, T> mapper);

    /**
     * Returns some collection of learned values since the seq-th round (inclusive) from the cluster.
     * This will attempt to communicate with all learner nodes in the cluster and will wait for consensus and should
     * collect at least {@code quorumResponses}.
     *
     * @param seq lower round cutoff for returned values
     * @return some set of learned values for rounds since the seq-th round
     * @see PaxosLearner#getLearnedValuesSince
     */
    PaxosResponses<PaxosUpdate> getLearnedValuesSince(long seq);
}
