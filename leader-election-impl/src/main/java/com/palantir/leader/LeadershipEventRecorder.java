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

import com.google.common.annotations.VisibleForTesting;
import com.palantir.paxos.PaxosRoundFailureException;
import com.palantir.paxos.PaxosValue;

public class LeadershipEventRecorder {

    private PaxosValue currentRound = null;
    private boolean hasLostLeadershipForCurrentRound = false;
    private String leaderId;

    private final LeadershipEvents events;

    public LeadershipEventRecorder(String leaderId) {
        this(new LeadershipEvents(), leaderId);
    }

    @VisibleForTesting
    LeadershipEventRecorder(LeadershipEvents events, String leaderUuid) {
        this.events = events;
        this.leaderId = leaderUuid;
    }

    public void recordProposalAttempt(long round) {
        events.proposedLeadershipFor(round);
    }

    public void recordProposalFailure(PaxosRoundFailureException e) {
        events.proposalFailure(e);
    }

    public synchronized void recordRound(PaxosValue round) {
        if (round == null) {
            return;
        }

        // ignore old rounds and duplicates, since threads may be trying to verify leadership for various rounds concurrently
        if (!isNewRound(round)) {
            return;
        }

        if (isLeaderFor(round)) {
            events.gainedLeadershipFor(round);
        } else if (isLeaderFor(currentRound) && !hasLostLeadershipForCurrentRound) {
            events.lostLeadershipFor(currentRound);
        }

        hasLostLeadershipForCurrentRound = false;
        currentRound = round;
    }

    public synchronized void recordNoQuorum(PaxosValue value) {
        recordRound(value);

        if (isLeaderFor(value)) {
            events.noQuorum(value);
        }
    }

    public synchronized void recordNotLeading(PaxosValue value) {
        recordRound(value);

        if (isSameRound(value) && isLeaderFor(value) && !hasLostLeadershipForCurrentRound) {
            hasLostLeadershipForCurrentRound = true;
            events.lostLeadershipFor(value);
        }
    }

    private boolean isLeaderFor(PaxosValue round) {
        return round != null && leaderId.equals(round.getLeaderUUID());
    }

    private boolean isNewRound(PaxosValue value) {
        return currentRound == null || value.getRound() > currentRound.getRound();
    }

    private boolean isSameRound(PaxosValue value) {
        return currentRound != null && value != null && currentRound.getRound() == value.getRound();
    }

}