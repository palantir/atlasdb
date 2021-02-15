/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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

import com.palantir.atlasdb.metrics.Timed;
import javax.annotation.Nullable;

public interface PaxosProposer {

    /**
     * Reaches a consensus with peers on a value of type V using a single instance of paxos. The
     * proposer is required to update its own local learned state before returning.
     *
     * @param seq the number identifying this instance of paxos
     * @param proposalValue default value to propose to the quorum
     * @return the value accepted by the quorum.  This may not be the value you have proposed
     * @throws PaxosRoundFailureException if the proposal round fails. The primary reasons a round
     *         will fail are (1) the proposer cannot reach a quorum of acceptors and (2) the
     *         sequence number is too old (acceptor logs have been truncated). We do not place
     *         restrictions on the proposed sequence number, but if a proposed sequence number
     *         leaves a gap in the sequence domain make sure the proposer/acceptors/listeners are
     *         equipped to handle this case. PaxosProposerImpl will not throw in this case,
     *         but requires it be enforced on a higher level.
     */
    @Timed
    byte[] propose(long seq, @Nullable byte[] proposalValue) throws PaxosRoundFailureException;

    /**
     * Reaches a consensus with peers on a value of type V using a single instance of paxos, as in
     * {@link PaxosProposer#propose(long, byte[])}. However, this value is proposed anonymously, with a fresh
     * proposer ID. This may be useful for relinquishing leadership in algorithms for that based on Paxos.
     */
    @Timed
    byte[] proposeAnonymously(long seq, @Nullable byte[] proposalValue) throws PaxosRoundFailureException;

    /**
     * Returns a unique string identifier for the proposer.
     */
    String getUuid();
}
