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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.common.proxy.DelegatingInvocationHandler;
import com.palantir.leader.LeaderElectionService;
import com.palantir.leader.LeaderElectionService.LeadershipToken;
import com.palantir.leader.LeaderElectionService.StillLeadingStatus;

public class PaxosConsensusFastTest {

    private final int NUM_POTENTIAL_LEADERS = 6;
    private final int QUORUM_SIZE = 4;

    List<LeaderElectionService> leaders = Lists.newArrayList();
    List<PaxosAcceptor> acceptors = Lists.newArrayList();
    List<PaxosLearner> learners = Lists.newArrayList();
    List<AtomicBoolean> failureToggles = Lists.newArrayList();

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
                leaders.get(leaderNum).isStillLeading(t) != StillLeadingStatus.NOT_LEADING);
        return t;
    }

    public void godown(int i) {
        failureToggles.get(i).set(true);
    }

    public void comeup(int i) {
        failureToggles.get(i).set(false);
    }

    @Test
    public void singleProposal() {
        gainLeadership(0);
    }

    @Test
    public void singleProposal2() {
        gainLeadership(2);
    }

    @Test
    public void changeLeadership() {
        gainLeadership(4);
        gainLeadership(2);
    }

    @Test
    public void leaderFailure1() {
        gainLeadership(4);
        godown(4);
        gainLeadership(5);
    }

    @Test
    public void leaderFailure2() {
        godown(2);
        gainLeadership(3);
        godown(3);
        comeup(2);
        gainLeadership(2);
    }

    @Test
    public void loseQuorum() {
        LeadershipToken t = gainLeadership(0);
        for (int i = 1; i < NUM_POTENTIAL_LEADERS - QUORUM_SIZE + 2; i++) {
            godown(i);
        }
        assertFalse(
                "leader cannot maintain leadership withou quorum",
                leaders.get(0).isStillLeading(t) == StillLeadingStatus.LEADING);
        comeup(1);
        gainLeadership(0);
        assertTrue("leader can confirm leadership with quorum", leaders.get(0).isStillLeading(t) != StillLeadingStatus.NOT_LEADING);
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
            godown(i);
        }
        LeadershipToken t = gainLeadership(0);
        godown(QUORUM_SIZE-1);
        ExecutorService exec = PTExecutors.newSingleThreadExecutor();
        Future<Void> f = exec.submit(new Callable<Void>() {
            @Override
            public Void call() {
                int i = QUORUM_SIZE-1;
                while (!Thread.currentThread().isInterrupted()) {
                    int next = i+1;
                    if (next == NUM_POTENTIAL_LEADERS) {
                        next = QUORUM_SIZE-1;
                    }
                    godown(next);
                    comeup(i);
                    i = next;
                }
                return null;
            }
        });
        LeadershipToken token2 = gainLeadership(0);
        assertTrue("leader can confirm leadership with quorum", t.sameAs(token2));
        f.cancel(true);
        exec.shutdown();
        exec.awaitTermination(10, TimeUnit.SECONDS);
        for (int i = 0; i < NUM_POTENTIAL_LEADERS ; i++) {
            comeup(i);
        }
    }

    @Test
    public void simpleLogTest() {
        String leaderUUID = "I-AM-DA-LEADER";
        String dir = "log-test";
        long seq = 0;

        // write to log
        PaxosStateLog<PaxosValue> log = new PaxosStateLogImpl<PaxosValue>(dir);
        log.writeRound(seq, new PaxosValue(leaderUUID, 0, null));

        // read back from log
        try {
            byte[] bytes = log.readRound(seq);
            assertNotNull(bytes);
            PaxosValue p = PaxosValue.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            assertTrue(p.getLeaderUUID().equals(leaderUUID));
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
            gainLeadership(i % NUM_POTENTIAL_LEADERS);
        }
        PaxosLearnerImpl learner = (PaxosLearnerImpl)
                ((DelegatingInvocationHandler) Proxy.getInvocationHandler(learners.get(0))).getDelegate();
        PaxosStateLog<PaxosValue> log = learner.log;
        SortedMap<Long, PaxosValue> cache = learner.state;
        log.truncate(log.getGreatestLogEntry());
        cache.clear();
        gainLeadership(0);
    }
}

