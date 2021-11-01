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
package com.palantir.leader;

import com.palantir.paxos.PaxosRoundFailureException;
import com.palantir.paxos.PaxosValue;
import com.palantir.sls.versions.OrderableSlsVersion;

public interface PaxosLeaderElectionEventRecorder {

    /** Called when a quorum no longer agrees that {@code value} is the latest round,
     *  although we are the leader for {@code value}. */
    void recordNotLeading(PaxosValue value);

    /** Called when we cannot contact a quorum to determine whether {@code value} is the latest round. */
    void recordNoQuorum(PaxosValue value);

    /** Called when we fail to propose a new value. */
    void recordProposalFailure(PaxosRoundFailureException paxosException);

    /** Called when we attempt to propose a new value. */
    void recordProposalAttempt(long round);

    /** Called when we are unable to ping the current leader. */
    void recordLeaderPingFailure(Throwable error);

    /** Called when an attempt to ping the current leader times out. */
    void recordLeaderPingTimeout();

    /** Called when we successfully contacted the suspected leader, but it reported that it was not the leader. */
    void recordLeaderPingReturnedFalse();

    /** Called when we successfully contacted the leader, but it is on an older version of TimeLock
     * as compared to the local server. */
    void recordLeaderOnOlderVersion(OrderableSlsVersion version);

    PaxosLeaderElectionEventRecorder NO_OP = new PaxosLeaderElectionEventRecorder() {
        @Override
        public void recordNotLeading(PaxosValue _value) {}

        @Override
        public void recordNoQuorum(PaxosValue _value) {}

        @Override
        public void recordProposalFailure(PaxosRoundFailureException _paxosException) {}

        @Override
        public void recordProposalAttempt(long _round) {}

        @Override
        public void recordLeaderPingFailure(Throwable _error) {}

        @Override
        public void recordLeaderPingTimeout() {}

        @Override
        public void recordLeaderPingReturnedFalse() {}

        @Override
        public void recordLeaderOnOlderVersion(OrderableSlsVersion _version) {}
    };
}
