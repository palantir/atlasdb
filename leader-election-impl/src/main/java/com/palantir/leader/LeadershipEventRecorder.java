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

import com.google.common.annotations.VisibleForTesting;
import com.palantir.paxos.PaxosRoundFailureException;
import com.palantir.paxos.PaxosValue;

public class LeadershipEventRecorder implements PaxosKnowledgeEventRecorder, PaxosLeaderElectionEventRecorder {

    private final String leaderId;
    private final LeadershipEvents events;

    @GuardedBy("this") private State state = new NotLeading();
    @GuardedBy("this") private PaxosValue currentRound;

    public LeadershipEventRecorder(String leaderId) {
        this(new LeadershipEvents(), leaderId);
    }

    @VisibleForTesting
    LeadershipEventRecorder(LeadershipEvents events, String leaderUuid) {
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
            state = state.newRound(round);
            currentRound = round;
        }
    }

    @Override
    public synchronized void recordNotLeading(PaxosValue value) {
        if (isSameRound(value)) {
            state = state.lostLeadership();
        }
    }

    @Override
    public synchronized void recordNoQuorum(PaxosValue value) {
        if (isSameRound(value)) {
            events.noQuorum(value);
        }
    }

    private boolean isNewRound(PaxosValue value) {
        return value != null && (currentRound == null || value.getRound() > currentRound.getRound());
    }

    private boolean isLeaderFor(PaxosValue round) {
        return round != null && leaderId.equals(round.getLeaderUUID());
    }

    private boolean isSameRound(PaxosValue value) {
        return currentRound != null && value != null && currentRound.getRound() == value.getRound();
    }

    private abstract class State {

        State newRound(PaxosValue value) {
            newRound();

            if (isLeaderFor(value)) {
                events.gainedLeadershipFor(value);
                return new Leading();
            }

            return new NotLeading();
        }

        abstract void newRound();
        abstract State lostLeadership();
    }

    private class Leading extends State {

        @Override
        void newRound() {
            events.lostLeadershipFor(currentRound);
        }

        @Override
        public State lostLeadership() {
            events.lostLeadershipFor(currentRound);
            return new NotLeading();
        }
    }

    private class NotLeading extends State {

        @Override
        void newRound() {
            // no event
        }

        @Override
        public State lostLeadership() {
            return this;
        }
    }

}