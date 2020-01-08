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

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.SafeArg;
import com.palantir.paxos.PaxosRoundFailureException;
import com.palantir.paxos.PaxosValue;
import com.palantir.tritium.metrics.registry.MetricName;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

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
    private final Object[] contextArgs;

    LeadershipEvents(
            TaggedMetricRegistry metrics,
            List<SafeArg<String>> safeLoggingArgs,
            List<SafeArg<String>> safeMetricArgs) {
        Map<String, String> safeTags = safeTags(safeMetricArgs);
        gainedLeadership = metrics.meter(withName("leadership.gained", safeTags));
        lostLeadership = metrics.meter(withName("leadership.lost", safeTags));
        noQuorum = metrics.meter(withName("leadership.no-quorum", safeTags));
        proposedLeadership = metrics.meter(withName("leadership.proposed", safeTags));
        proposalFailure = metrics.meter(withName("leadership.proposed.failure", safeTags));
        leaderPingFailure = metrics.meter(withName("leadership.ping-leader.failure", safeTags));
        leaderPingTimeout = metrics.meter(withName("leadership.ping-leader.timeout", safeTags));
        leaderPingReturnedFalse = metrics.meter(withName("leadership.ping-leader.returned-false", safeTags));
        this.contextArgs = safeLoggingArgs.toArray(new Object[0]);
    }

    void proposedLeadershipFor(long round) {
        leaderLog.info("Proposing leadership for {}", withContextArgs(SafeArg.of("round", round)));
        proposedLeadership.mark();
    }

    void gainedLeadershipFor(PaxosValue value) {
        leaderLog.info("Gained leadership for {}", withContextArgs(SafeArg.of("value", value)));
        gainedLeadership.mark();
    }

    void lostLeadershipFor(PaxosValue value) {
        leaderLog.info("Lost leadership for {}", withContextArgs(SafeArg.of("value", value)));
        lostLeadership.mark();
    }

    void noQuorum(PaxosValue value) {
        leaderLog.warn("The most recent known information says this server is the leader,"
                        + " but there is no quorum right now. The paxos value is {}",
                withContextArgs(SafeArg.of("value", value)));
        noQuorum.mark();
    }

    void leaderPingFailure(Throwable error) {
        leaderLog.warn("Failed to ping the current leader", withContextArgs(error));
        leaderPingFailure.mark();
    }

    void leaderPingTimeout() {
        leaderLog.warn("Timed out while attempting to ping the current leader", contextArgs);
        leaderPingTimeout.mark();
    }

    void leaderPingReturnedFalse() {
        leaderLog.info("We contacted the suspected leader, but it reported that it was no longer leading", contextArgs);
        leaderPingReturnedFalse.mark();
    }

    void proposalFailure(PaxosRoundFailureException paxosException) {
        leaderLog.warn("Leadership was not gained.\n"
                        + "We should recover automatically. If this recurs often, try to \n"
                        + "  (1) ensure that most other nodes are reachable over the network, and \n"
                        + "  (2) increase the randomWaitBeforeProposingLeadershipMs timeout in your configuration.",
                withContextArgs(paxosException));
        proposalFailure.mark();
    }

    private Object[] withContextArgs(Object arg) {
        if (contextArgs.length == 0) {
            return new Object[] { arg };
        } else {
            return ArrayUtils.add(contextArgs, arg);
        }
    }

    private static MetricName withName(String name, Map<String, String> safeTags) {
        return MetricName.builder()
                .safeName(name)
                .safeTags(safeTags)
                .build();
    }

    private static Map<String, String> safeTags(List<SafeArg<String>> contextArgs) {
        return KeyedStream.of(contextArgs)
                .mapKeys(SafeArg::getName)
                .map(SafeArg::getValue)
                .collectToMap();
    }
}
