/**
 * Copyright 2015 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.paxos;

import javax.annotation.Nullable;

public interface PaxosProposer {

    /**
     * Reaches a consensus with peers on a value of type V using a single instance of paxos. The
     * proposer is required to update its own local learned state before returning.
     *
     * @param seq the number identifying this instance of paxos
     * @param proposalValue default value to propose to the quorum
     * @return the value accepted by the quorum.  This may not be the value you have proposed.
     * @throws PaxosRoundFailureException if the proposal round fails. The primary reasons a round
     *         will fail are (1) the proposer cannot reach a quorum of acceptors and (2) the
     *         sequence number is too old (acceptor logs have been truncated). We do not place
     *         restrictions on the proposed sequence number, but if a proposed sequence number
     *         leaves a gap in the sequence domain make sure the proposer/acceptors/listeners are
     *         equipped to handle this case. PaxosProposerImpl will not throw in this case,
     *         but requires it be enforced on a higher level.
     */
    public byte[] propose(long seq, @Nullable byte[] proposalValue)
            throws PaxosRoundFailureException;

    /**
     * @return the number of acceptors that need to support a successful request
     */
    public int getQuorumSize();

    /**
     * @return a unique string identifier for the proposer
     */
    public String getUUID();
}
