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

import com.google.common.util.concurrent.RateLimiter;
import com.palantir.logsafe.SafeArg;
import com.palantir.paxos.PaxosRoundFailureException;
import com.palantir.paxos.PaxosValue;
import com.palantir.sls.versions.OrderableSlsVersion;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import java.util.List;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
class LeadershipEvents {

    private static final String LEADER_LOG_NAME = "leadership";
    @SuppressWarnings("PreferSafeLogger") // Some refactoring required
    private static final Logger leaderLog = LoggerFactory.getLogger(LEADER_LOG_NAME);
    private final Object[] contextArgs;
    private final LeaderElectionServiceMetrics leaderElectionServiceMetrics;
    private final RateLimiter leaderOnOlderVersionLoggingRateLimiter = RateLimiter.create(0.03);

    LeadershipEvents(TaggedMetricRegistry metrics, List<SafeArg<String>> safeLoggingArgs) {
        leaderElectionServiceMetrics = LeaderElectionServiceMetrics.of(metrics);
        this.contextArgs = safeLoggingArgs.toArray(new Object[0]);
    }

    void proposedLeadershipFor(long round) {
        leaderLog.info("Proposing leadership for {}", withContextArgs(SafeArg.of("round", round)));
        leaderElectionServiceMetrics.proposedLeadership().mark();
    }

    void gainedLeadershipFor(PaxosValue value) {
        leaderLog.info("Gained leadership for {}", withContextArgs(SafeArg.of("value", value)));
        leaderElectionServiceMetrics.gainedLeadership().mark();
    }

    void lostLeadershipFor(PaxosValue value) {
        leaderLog.info("Lost leadership for {}", withContextArgs(SafeArg.of("value", value)));
        leaderElectionServiceMetrics.lostLeadership().mark();
    }

    void noQuorum(PaxosValue value) {
        leaderLog.warn(
                "The most recent known information says this server is the leader,"
                        + " but there is no quorum right now. The paxos value is {}",
                withContextArgs(SafeArg.of("value", value)));
        leaderElectionServiceMetrics.noQuorum().mark();
    }

    void leaderPingFailure(Throwable error) {
        leaderLog.warn("Failed to ping the current leader", withContextArgs(error));
        leaderElectionServiceMetrics.leaderPingFailure().mark();
    }

    void leaderPingTimeout() {
        leaderLog.warn("Timed out while attempting to ping the current leader", contextArgs);
        leaderElectionServiceMetrics.leaderPingTimeout().mark();
    }

    void leaderPingReturnedFalse() {
        leaderLog.info("We contacted the suspected leader, but it reported that it was no longer leading", contextArgs);
        leaderElectionServiceMetrics.leaderPingReturnedFalse().mark();
    }

    void proposalFailure(PaxosRoundFailureException paxosException) {
        leaderLog.warn(
                "Leadership was not gained.\n"
                        + "We should recover automatically. If this recurs often, try to \n"
                        + "  (1) ensure that most other nodes are reachable over the network, and \n"
                        + "  (2) increase the randomWaitBeforeProposingLeadershipMs timeout in your configuration.",
                withContextArgs(paxosException));
        leaderElectionServiceMetrics.proposalFailure().mark();
    }

    public void leaderOnOlderTimeLockVersion(OrderableSlsVersion version) {
        // TODO(snanda): Kill log after few successful runs of blue-green deployment.
        if (leaderOnOlderVersionLoggingRateLimiter.tryAcquire()) {
            leaderLog.info(
                    "We contacted the leader and it reported that it is on an older version of TimeLock - {}",
                    withContextArgs(SafeArg.of("version", version)));
        }
        leaderElectionServiceMetrics.leaderOnOlderTimeLockVersion().mark();
    }

    private Object[] withContextArgs(Object arg) {
        if (contextArgs.length == 0) {
            return new Object[] {arg};
        } else {
            return ArrayUtils.add(contextArgs, arg);
        }
    }
}
