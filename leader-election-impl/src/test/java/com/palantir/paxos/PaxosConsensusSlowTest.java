/**
 * Copyright 2015 Palantir Technologies
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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.leader.LeaderElectionService;
import com.palantir.leader.LeaderElectionService.LeadershipToken;
import com.palantir.leader.LeaderElectionService.StillLeadingStatus;

public class PaxosConsensusSlowTest {

    private final int NUM_POTENTIAL_LEADERS = 6;
    private final int QUORUM_SIZE = 4;

    Executor executor = PTExecutors.newCachedThreadPool();
    List<LeaderElectionService> leaders = new ArrayList<LeaderElectionService>();
    List<PaxosAcceptor> acceptors = Lists.newArrayList();
    List<PaxosLearner> learners = Lists.newArrayList();
    List<AtomicBoolean> failureToggles = new ArrayList<AtomicBoolean>();

    @Before
    public void setup() {
        PaxosConsensusTestUtils.setup(
                NUM_POTENTIAL_LEADERS,
                QUORUM_SIZE,
                leaders,
                acceptors,
                learners,
                failureToggles);
    }

    @After
    public void teardown() {
        PaxosConsensusTestUtils.teardown();
    }


    public LeadershipToken gainLeadership(int leaderNum) {
        LeadershipToken t = null;
        try {
            t = leaders.get(leaderNum).blockOnBecomingLeader();
        } catch (InterruptedException e) {
            fail(e.getMessage());
        }
        assertTrue(
                "leader should still be leading right after becoming leader",
                leaders.get(leaderNum).isStillLeading(t) != StillLeadingStatus.NO_QUORUM);
        return t;
    }

    public void godown(int i) {
        failureToggles.get(i).set(true);
    }

    public void comeup(int i) {
        failureToggles.get(i).set(false);
    }

    static final long NO_QUORUM_POLL_WAIT_TIME_IN_MS = 100;
    static final long QUORUM_POLL_WAIT_TIME_IN_MS = 30000;

    @Test
    public void waitingOnQuorum() {
        for (int i = 0; i < NUM_POTENTIAL_LEADERS - 1; i++) {
            godown(i);
        }

        CompletionService<Void> leadershipCompletionService = new ExecutorCompletionService<Void>(
                executor);
        leadershipCompletionService.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                gainLeadership(NUM_POTENTIAL_LEADERS - 1);
                return null;
            }
        });

        for (int i = 0; i < NUM_POTENTIAL_LEADERS - 1; i++) {
            try {
                if (i + 1 < QUORUM_SIZE) {
                    assertNull(
                            "proposer should continue to block without quorum",
                            leadershipCompletionService.poll(
                                    NO_QUORUM_POLL_WAIT_TIME_IN_MS,
                                    TimeUnit.MILLISECONDS));
                } else {
                    assertNotNull(
                            "proposer should get leadership with quorum",
                            leadershipCompletionService.poll(
                                    QUORUM_POLL_WAIT_TIME_IN_MS,
                                    TimeUnit.MILLISECONDS));
                    return;
                }
            } catch (InterruptedException e) {
            } finally {
                comeup(i);
            }
        }
    }

}
