/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.palantir.logsafe.SafeArg;
import com.palantir.paxos.PaxosRoundFailureException;
import com.palantir.paxos.PaxosValue;

import net.jcip.annotations.ThreadSafe;

@ThreadSafe
class LeadershipEvents {

    private static final String LEADER_LOG_NAME = "leadership";
    private static final Logger leaderLog = LoggerFactory.getLogger(LEADER_LOG_NAME);

    private final Meter gainedLeadership;
    private final Meter lostLeadership;
    private final Meter noQuorum;
    private final Meter proposedLeadership;
    private final Meter proposalFailure;
    private final Meter leaderPingFailure;
    private final Meter leaderPingTimeout;
    private final Meter leaderPingReturnedFalse;

    public LeadershipEvents(MetricRegistry metrics) {
        gainedLeadership = metrics.meter("leadership.gained");
        lostLeadership = metrics.meter("leadership.lost");
        noQuorum = metrics.meter("leadership.no-quorum");
        proposedLeadership = metrics.meter("leadership.proposed");
        proposalFailure = metrics.meter("leadership.proposed.failure");
        leaderPingFailure = metrics.meter("leadership.ping-leader.failure");
        leaderPingTimeout = metrics.meter("leadership.ping-leader.timeout");
        leaderPingReturnedFalse = metrics.meter("leadership.ping-leader.returned-false");
    }

    public void proposedLeadershipFor(long round) {
        leaderLog.info("Proposing leadership for {}", SafeArg.of("round", round));
        proposedLeadership.mark();
    }

    public void gainedLeadershipFor(PaxosValue value) {
        leaderLog.info("Gained leadership for {}", SafeArg.of("value", value));
        gainedLeadership.mark();
    }

    public void lostLeadershipFor(PaxosValue value) {
        leaderLog.info("Lost leadership for {}", SafeArg.of("value", value));
        lostLeadership.mark();
    }

    public void noQuorum(PaxosValue value) {
        leaderLog.warn("The most recent known information says this server is the leader,"
                        + " but there is no quorum right now. The paxos value is {}",
                SafeArg.of("value", value));
        noQuorum.mark();
    }

    public void leaderPingFailure(Throwable error) {
        leaderLog.warn("Failed to ping the current leader", error);
        leaderPingFailure.mark();
    }

    public void leaderPingTimeout() {
        leaderLog.warn("Timed out while attempting to ping the current leader");
        leaderPingTimeout.mark();
    }

    public void leaderPingReturnedFalse() {
        leaderLog.info("We contacted the suspected leader, but it reported that it was no longer leading");
        leaderPingReturnedFalse.mark();
    }

    public void proposalFailure(PaxosRoundFailureException e) {
        leaderLog.warn("Leadership was not gained.\n"
                + "We should recover automatically. If this recurs often, try to \n"
                + "  (1) ensure that most other nodes are reachable over the network, and \n"
                + "  (2) increase the randomWaitBeforeProposingLeadershipMs timeout in your configuration.", e);
        proposalFailure.mark();
    }
}
