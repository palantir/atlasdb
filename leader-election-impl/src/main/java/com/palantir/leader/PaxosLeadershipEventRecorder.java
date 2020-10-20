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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.palantir.logsafe.SafeArg;
import com.palantir.paxos.PaxosRoundFailureException;
import com.palantir.paxos.PaxosValue;
import com.palantir.sls.versions.OrderableSlsVersion;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

public class PaxosLeadershipEventRecorder implements PaxosKnowledgeEventRecorder, PaxosLeaderElectionEventRecorder {

    private final String leaderId;
    private final LeadershipEvents events;
    private final Optional<LeadershipObserver> leadershipObserver;

    @GuardedBy("this")
    private PaxosValue currentRound = null;

    @GuardedBy("this")
    private boolean isLeading = false;

    public static PaxosLeadershipEventRecorder create(TaggedMetricRegistry metrics, String leaderUuid) {
        return create(metrics, leaderUuid, null, ImmutableList.of());
    }

    public static PaxosLeadershipEventRecorder create(
            TaggedMetricRegistry metrics,
            String leaderUuid,
            @Nullable LeadershipObserver observer,
            List<SafeArg<String>> safeLoggingArgs) {
        return new PaxosLeadershipEventRecorder(
                new LeadershipEvents(metrics, safeLoggingArgs), leaderUuid, Optional.ofNullable(observer));
    }

    @VisibleForTesting
    PaxosLeadershipEventRecorder(LeadershipEvents events, String leaderUuid, Optional<LeadershipObserver> observer) {
        this.events = events;
        this.leaderId = leaderUuid;
        this.leadershipObserver = observer;
    }

    @Override
    public void recordProposalAttempt(long round) {
        events.proposedLeadershipFor(round);
    }

    @Override
    public void recordLeaderPingFailure(Throwable error) {
        events.leaderPingFailure(error);
    }

    @Override
    public void recordLeaderPingTimeout() {
        events.leaderPingTimeout();
    }

    @Override
    public void recordLeaderPingReturnedFalse() {
        events.leaderPingReturnedFalse();
    }

    @Override
    public void recordLeaderOnOlderVersion(OrderableSlsVersion version) {
        events.leaderOnOlderTimeLockVersion(version);
    }

    @Override
    public void recordProposalFailure(PaxosRoundFailureException paxosException) {
        events.proposalFailure(paxosException);
    }

    @Override
    public synchronized void recordRound(PaxosValue round) {
        if (isNewRound(round)) {
            recordNewRound(round);
        }
    }

    private synchronized void recordNewRound(PaxosValue round) {
        if (isLeading) {
            events.lostLeadershipFor(currentRound);
            leadershipObserver.ifPresent(LeadershipObserver::lostLeadership);
        }

        if (isLeaderFor(round)) {
            events.gainedLeadershipFor(round);
            leadershipObserver.ifPresent(LeadershipObserver::gainedLeadership);
        }

        currentRound = round;
        isLeading = isLeaderFor(round);
    }

    @Override
    public synchronized void recordNotLeading(PaxosValue value) {
        if (isSameRound(value) && isLeading) {
            events.lostLeadershipFor(value);
            leadershipObserver.ifPresent(LeadershipObserver::lostLeadership);
            isLeading = false;
        }
    }

    @Override
    public synchronized void recordNoQuorum(PaxosValue value) {
        if (isSameRound(value)) {
            events.noQuorum(value);
        }
    }

    private synchronized boolean isNewRound(PaxosValue value) {
        return value != null && (currentRound == null || value.getRound() > currentRound.getRound());
    }

    private synchronized boolean isLeaderFor(PaxosValue round) {
        return round != null && leaderId.equals(round.getLeaderUUID());
    }

    private synchronized boolean isSameRound(PaxosValue value) {
        return currentRound != null && value != null && currentRound.getRound() == value.getRound();
    }
}
