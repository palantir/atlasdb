/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.leader;

import com.palantir.paxos.PaxosRoundFailureException;
import com.palantir.paxos.PaxosValue;

public interface PaxosLeaderElectionEventRecorder {

    void recordNotLeading(PaxosValue value);

    void recordNoQuorum(PaxosValue value);

    void recordProposalFailure(PaxosRoundFailureException e);

    void recordProposalAttempt(long round);

    PaxosLeaderElectionEventRecorder NO_OP = new PaxosLeaderElectionEventRecorder() {
        @Override
        public void recordNotLeading(PaxosValue value) { }

        @Override
        public void recordNoQuorum(PaxosValue value) { }

        @Override
        public void recordProposalFailure(PaxosRoundFailureException e) { }

        @Override
        public void recordProposalAttempt(long round) { }
    };

}
