/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.paxos;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import com.palantir.leader.LeaderElectionService;
import com.palantir.leader.LeaderElectionService.LeadershipToken;
import com.palantir.leader.LeaderElectionService.StillLeadingStatus;

public class PaxosTestState {
    private final List<LeaderElectionService> leaders;
    private final List<PaxosAcceptor> acceptors;
    private final List<PaxosLearner> learners;
    private final List<AtomicBoolean> failureToggles;
    private final ExecutorService executor;

    public PaxosTestState(List<LeaderElectionService> leaders, List<PaxosAcceptor> acceptors,
            List<PaxosLearner> learners, List<AtomicBoolean> failureToggles, ExecutorService executor) {
        this.leaders = leaders;
        this.acceptors = acceptors;
        this.learners = learners;
        this.failureToggles = failureToggles;
        this.executor = executor;
    }

    public void goDown(int idx) {
        failureToggles.get(idx).set(true);
    }

    public void comeUp(int idx) {
        failureToggles.get(idx).set(false);
    }

    public LeadershipToken gainLeadership(int leaderNum) {
        return gainLeadership(leaderNum, true /* check leadership afterwards */);
    }

    public LeadershipToken gainLeadership(int leaderNum, boolean checkAfterwards) {
        LeaderElectionService.LeadershipToken token = null;
        try {
            token = leader(leaderNum).blockOnBecomingLeader();
        } catch (InterruptedException e) {
            fail(e.getMessage());
        }
        if (checkAfterwards) {
            assertEquals(
                    "leader should still be leading right after becoming leader",
                    StillLeadingStatus.LEADING,
                    leader(leaderNum).isStillLeading(token));
        }
        return token;
    }

    public LeadershipToken gainLeadershipWithoutCheckingAfter(int leaderNum) {
        return gainLeadership(leaderNum, false /* check leadership afterwards */);
    }

    public LeaderElectionService leader(int idx) {
        return leaders.get(idx);
    }

    public PaxosLearner learner(int idx) {
        return learners.get(idx);
    }

    public ExecutorService getExecutor() {
        return executor;
    }
}
