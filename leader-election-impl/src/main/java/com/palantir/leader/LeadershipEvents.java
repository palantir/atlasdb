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

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.palantir.logsafe.SafeArg;
import com.palantir.paxos.PaxosRoundFailureException;
import com.palantir.paxos.PaxosValue;

class LeadershipEvents {

    private final Meter gainedLeadership;
    private final Meter lostLeadership;
    private final Meter noQuorum;
    private final Meter proposedLeadership;
    private final Meter proposalFailure;

    public LeadershipEvents() {
        this(LeaderLog.metrics());
    }

    private LeadershipEvents(MetricRegistry metrics) {
        gainedLeadership = metrics.meter("leadership.gained");
        lostLeadership = metrics.meter("leadership.lost");
        noQuorum = metrics.meter("leadership.no-quorum");
        proposedLeadership = metrics.meter("leadership.proposed");
        proposalFailure = metrics.meter("leadership.proposed.failure");
    }

    public void proposedLeadershipFor(long round) {
        proposedLeadership.mark();
        LeaderLog.logger.info("Proposing leadership with sequence number {}", round);
    }

    public void gainedLeadershipFor(PaxosValue value) {
        LeaderLog.logger.info("Gained leadership", SafeArg.of("value", value));
        gainedLeadership.mark();
    }

    public void lostLeadershipFor(PaxosValue value) {
        LeaderLog.logger.info("Lost leadership", SafeArg.of("value", value));
        lostLeadership.mark();
    }

    public void noQuorum(PaxosValue value) {
        LeaderLog.logger.warn("The most recent known information says this server is the leader, but there is no quorum right now");
        noQuorum.mark();
    }

    public void proposalFailure(PaxosRoundFailureException e) {
        LeaderLog.logger.warn("Leadership was not gained.\n"
                + "We should recover automatically. If this recurs often, try to \n"
                + "  (1) ensure that most other nodes are reachable over the network, and \n"
                + "  (2) increase the randomWaitBeforeProposingLeadershipMs timeout in your configuration.\n"
                + "See the debug-level log for more details.");
        LeaderLog.logger.debug("Specifically, leadership was not gained because of the following exception", e);
        proposalFailure.mark();
    }
}
