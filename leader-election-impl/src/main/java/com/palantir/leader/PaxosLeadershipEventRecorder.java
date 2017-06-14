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

import javax.annotation.concurrent.GuardedBy;

import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.palantir.paxos.PaxosRoundFailureException;
import com.palantir.paxos.PaxosValue;

public class PaxosLeadershipEventRecorder implements PaxosKnowledgeEventRecorder, PaxosLeaderElectionEventRecorder {

    private final String leaderId;
    private final LeadershipEvents events;

    @GuardedBy("this") private PaxosValue currentRound = null;
    @GuardedBy("this") private boolean isLeading = false;

    public static PaxosLeadershipEventRecorder create(MetricRegistry metrics, String leaderUuid) {
        return new PaxosLeadershipEventRecorder(new LeadershipEvents(metrics), leaderUuid);
    }

    @VisibleForTesting
    PaxosLeadershipEventRecorder(LeadershipEvents events, String leaderUuid) {
        this.events = events;
        this.leaderId = leaderUuid;
    }

    @Override
    public void recordProposalAttempt(long round) {
        events.proposedLeadershipFor(round);
    }

    @Override
    public void recordProposalFailure(PaxosRoundFailureException e) {
        events.proposalFailure(e);
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
        }

        if (isLeaderFor(round)) {
            events.gainedLeadershipFor(round);
        }

        currentRound = round;
        isLeading = isLeaderFor(round);
    }

    @Override
    public synchronized void recordNotLeading(PaxosValue value) {
        if (isSameRound(value) && isLeading) {
            events.lostLeadershipFor(value);
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