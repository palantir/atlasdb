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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.SortedMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.palantir.common.concurrent.PTExecutors;
import com.palantir.common.proxy.DelegatingInvocationHandler;
import com.palantir.leader.LeaderElectionService.LeadershipToken;
import com.palantir.leader.LeaderElectionService.StillLeadingStatus;

public class PaxosConsensusFastTest {

    private final int NUM_POTENTIAL_LEADERS = 6;
    private final int QUORUM_SIZE = 4;
    private PaxosTestState state;

    @Before
    public void setup() {
        state = PaxosConsensusTestUtils.setup(NUM_POTENTIAL_LEADERS, QUORUM_SIZE);
    }

    @After
    public void teardown() throws Exception {
        PaxosConsensusTestUtils.teardown(state);
    }

    @Test
    public void singleProposal() {
        state.gainLeadership(0);
    }

    @Test
    public void singleProposal2() {
        state.gainLeadership(2);
    }

    @Test
    public void changeLeadership() {
        state.gainLeadership(4);
        state.gainLeadership(2);
    }

    @Test
    public void leaderFailure1() {
        state.gainLeadership(4);
        state.goDown(4);
        state.gainLeadership(5);
    }

    @Test
    public void leaderFailure2() {
        state.goDown(2);
        state.gainLeadership(3);
        state.goDown(3);
        state.comeUp(2);
        state.gainLeadership(2);
    }

    @Test
    public void loseQuorum() {
        LeadershipToken t = state.gainLeadership(0);
        for (int i = 1; i < NUM_POTENTIAL_LEADERS - QUORUM_SIZE + 2; i++) {
            state.goDown(i);
        }
        assertFalse(
                "leader cannot maintain leadership withou quorum",
                state.leader(0).isStillLeading(t) == StillLeadingStatus.LEADING);
        state.comeUp(1);
        state.gainLeadership(0);
        assertTrue("leader can confirm leadership with quorum", state.leader(0).isStillLeading(t) != StillLeadingStatus.NOT_LEADING);
    }

    @Test
    public void loseQuorumMany() {
        for (int i = 0 ; i < 100 ; i++) {
            loseQuorum();
        }
    }

    @Test
    public void loseQuorumDiffTokenMany() throws InterruptedException {
        for (int i = 0 ; i < 100 ; i++) {
            loseQuorumDiffToken();
        }
    }

    @Test
    public void loseQuorumDiffToken() throws InterruptedException {
        for (int i = QUORUM_SIZE; i < NUM_POTENTIAL_LEADERS ; i++) {
            state.goDown(i);
        }
        LeadershipToken t = state.gainLeadership(0);
        state.goDown(QUORUM_SIZE - 1);
        ExecutorService exec = PTExecutors.newSingleThreadExecutor();
        Future<Void> f = exec.submit(() -> {
            int i = QUORUM_SIZE-1;
            while (!Thread.currentThread().isInterrupted()) {
                int next = i+1;
                if (next == NUM_POTENTIAL_LEADERS) {
                    next = QUORUM_SIZE-1;
                }
                state.goDown(next);
                state.comeUp(i);
                i = next;
            }
            return null;
        });
        // Don't check leadership immediately after gaining it, since quorum might get lost.
        LeadershipToken token2 = state.gainLeadershipWithoutCheckingAfter(0);
        assertTrue("leader can confirm leadership with quorum", t.sameAs(token2));
        f.cancel(true);
        exec.shutdown();
        exec.awaitTermination(10, TimeUnit.SECONDS);
        for (int i = 0; i < NUM_POTENTIAL_LEADERS ; i++) {
            state.comeUp(i);
        }
    }

    @Test
    public void simpleLogTest() {
        String leaderUuid = "I-AM-DA-LEADER";
        String dir = "log-test";
        long seq = 0;

        // write to log
        PaxosStateLog<PaxosValue> log = new PaxosStateLogImpl<PaxosValue>(dir);
        log.writeRound(seq, new PaxosValue(leaderUuid, 0, null));

        // read back from log
        try {
            byte[] bytes = log.readRound(seq);
            assertNotNull(bytes);
            PaxosValue p = PaxosValue.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            assertTrue(p.getLeaderUUID().equals(leaderUuid));
        } catch (IOException e1) {
            fail("IO exception when reading log");
        }

        // cleanup
        try {
            FileUtils.deleteDirectory(new File(dir));
        } catch (Exception e) {}
    }

    @Test
    public void learnerRecovery() {
        for (int i = 0; i < NUM_POTENTIAL_LEADERS * 3; i++) {
            state.gainLeadership(i % NUM_POTENTIAL_LEADERS);
        }
        PaxosLearnerImpl learner = (PaxosLearnerImpl)
                ((DelegatingInvocationHandler) Proxy.getInvocationHandler(state.learner(0))).getDelegate();
        PaxosStateLog<PaxosValue> log = learner.log;
        SortedMap<Long, PaxosValue> cache = learner.state;
        log.truncate(log.getGreatestLogEntry());
        cache.clear();
        state.gainLeadership(0);
    }
}

