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
package com.palantir.paxos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.fail;

import com.google.common.util.concurrent.Futures;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.common.proxy.DelegatingInvocationHandler;
import com.palantir.leader.LeaderElectionService.LeadershipToken;
import com.palantir.leader.LeaderElectionService.StillLeadingStatus;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.SortedMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PaxosConsensusFastTest {
    private PaxosTestState state;

    private static final int NUM_POTENTIAL_LEADERS = 6;
    private static final int QUORUM_SIZE = 4;

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
        LeadershipToken token = state.gainLeadership(0);
        knockOutQuorumNotIncludingZero();
        assertThat(StillLeadingStatus.LEADING)
                .describedAs("leader cannot maintain leadership without quorum")
                .isNotSameAs(state.leader(0).isStillLeading(token));
        state.comeUp(1);
        state.gainLeadership(0);
        assertThat(StillLeadingStatus.NOT_LEADING)
                .describedAs("leader can confirm leadership with quorum")
                .isNotSameAs(state.leader(0).isStillLeading(token));
    }

    @Test
    public void loseQuorumMany() {
        for (int i = 0; i < 100; i++) {
            loseQuorum();
        }
    }

    @Test
    public void loseQuorumDiffTokenMany() throws InterruptedException {
        for (int i = 0; i < 100; i++) {
            loseQuorumDiffToken();
        }
    }

    @Test
    public void loseQuorumDiffToken() throws InterruptedException {
        for (int i = QUORUM_SIZE; i < NUM_POTENTIAL_LEADERS; i++) {
            state.goDown(i);
        }
        LeadershipToken token = state.gainLeadership(0);
        state.goDown(QUORUM_SIZE - 1);
        ExecutorService exec = PTExecutors.newSingleThreadExecutor();
        Future<Void> future = exec.submit(() -> {
            int it = QUORUM_SIZE - 1;
            while (!Thread.currentThread().isInterrupted()) {
                int next = it + 1;
                if (next == NUM_POTENTIAL_LEADERS) {
                    next = QUORUM_SIZE - 1;
                }
                state.goDown(next);
                state.comeUp(it);
                it = next;
            }
            return null;
        });
        // Don't check leadership immediately after gaining it, since quorum might get lost.
        LeadershipToken token2 = state.gainLeadershipWithoutCheckingAfter(0);
        assertThat(token.sameAs(token2))
                .describedAs("leader can confirm leadership with quorum")
                .isTrue();
        future.cancel(true);
        exec.shutdown();
        exec.awaitTermination(10, TimeUnit.SECONDS);
        restoreAllNodes();
    }

    @SuppressWarnings("EmptyCatchBlock")
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
            assertThat(bytes).isNotNull();
            PaxosValue paxosValue = PaxosValue.BYTES_HYDRATOR.hydrateFromBytes(bytes);
            assertThat(leaderUuid).isEqualTo(paxosValue.getLeaderUUID());
        } catch (IOException e1) {
            fail("IO exception when reading log");
        }

        // cleanup
        try {
            FileUtils.deleteDirectory(new File(dir));
        } catch (Exception ignored) {
        }
    }

    @Test
    public void learnerRecovery() {
        for (int i = 0; i < NUM_POTENTIAL_LEADERS * 3; i++) {
            state.gainLeadership(i % NUM_POTENTIAL_LEADERS);
        }
        PaxosLearnerImpl learner = (PaxosLearnerImpl)
                ((DelegatingInvocationHandler) Proxy.getInvocationHandler(state.learner(0))).getDelegate();
        PaxosStateLog<PaxosValue> log = learner.learnerStateLog;
        SortedMap<Long, PaxosValue> cache = learner.state;
        log.truncate(log.getGreatestLogEntry());
        cache.clear();
        state.gainLeadership(0);
    }

    @Test
    public void loseLeadershipAfterSteppingDown() {
        LeadershipToken token = state.gainLeadership(0);
        assertThat(Futures.getUnchecked(state.leader(0).isStillLeading(token))).isEqualTo(StillLeadingStatus.LEADING);
        assertThat(state.leader(0).stepDown()).isTrue();
        assertThat(Futures.getUnchecked(state.leader(0).isStillLeading(token)))
                .isEqualTo(StillLeadingStatus.NOT_LEADING);
    }

    @Test
    public void gainLeadershipAfterHostileTakeover() {
        LeadershipToken token1 = state.gainLeadership(0);
        assertThat(Futures.getUnchecked(state.leader(0).isStillLeading(token1))).isEqualTo(StillLeadingStatus.LEADING);

        Awaitility.waitAtMost(Duration.ofSeconds(5)).until(() -> state.leader(1).hostileTakeover());
        assertThat(Futures.getUnchecked(state.leader(0).isStillLeading(token1)))
                .isEqualTo(StillLeadingStatus.NOT_LEADING);
        assertThat(state.leader(1).getCurrentTokenIfLeading()).isNotEmpty();
    }

    @Test
    public void otherNodeCanBecomeLeaderAfterSteppingDown() {
        LeadershipToken token = state.gainLeadership(0);
        assertThat(Futures.getUnchecked(state.leader(0).isStillLeading(token))).isEqualTo(StillLeadingStatus.LEADING);
        assertThat(state.leader(0).stepDown()).isTrue();

        LeadershipToken token2 = state.gainLeadership(1);
        assertThat(Futures.getUnchecked(state.leader(0).isStillLeading(token)))
                .isEqualTo(StillLeadingStatus.NOT_LEADING);
        assertThat(Futures.getUnchecked(state.leader(1).isStillLeading(token2))).isEqualTo(StillLeadingStatus.LEADING);
    }

    @Test
    public void leadershipIfRegainedImmediatelyIsOnDifferentToken() {
        LeadershipToken token1 = state.gainLeadership(0);
        assertThat(Futures.getUnchecked(state.leader(0).isStillLeading(token1))).isEqualTo(StillLeadingStatus.LEADING);
        assertThat(state.leader(0).stepDown()).isTrue();

        LeadershipToken token2 = state.gainLeadership(0);
        assertThat(Futures.getUnchecked(state.leader(0).isStillLeading(token1)))
                .isEqualTo(StillLeadingStatus.NOT_LEADING);
        assertThat(Futures.getUnchecked(state.leader(0).isStillLeading(token2))).isEqualTo(StillLeadingStatus.LEADING);
        assertThat(state.leader(0).stepDown()).isTrue();
    }

    @Test
    public void nonLeaderStepDownDoesNotAffectLeader() {
        LeadershipToken token = state.gainLeadership(0);
        assertThat(Futures.getUnchecked(state.leader(0).isStillLeading(token))).isEqualTo(StillLeadingStatus.LEADING);
        assertThat(state.leader(1).stepDown()).isFalse();
        assertThat(Futures.getUnchecked(state.leader(0).isStillLeading(token))).isEqualTo(StillLeadingStatus.LEADING);
    }

    @Test
    public void failToStepDownIfNoQuorum() {
        LeadershipToken token = state.gainLeadership(0);
        assertThat(Futures.getUnchecked(state.leader(0).isStillLeading(token))).isEqualTo(StillLeadingStatus.LEADING);

        knockOutQuorumNotIncludingZero();
        assertThat(state.leader(0).stepDown()).isFalse();

        restoreAllNodes();
        assertThat(Futures.getUnchecked(state.leader(0).isStillLeading(token))).isEqualTo(StillLeadingStatus.LEADING);
    }

    @Test
    public void failToTakeOverIfNoQuorum() {
        LeadershipToken token = state.gainLeadership(1);
        assertThat(Futures.getUnchecked(state.leader(1).isStillLeading(token))).isEqualTo(StillLeadingStatus.LEADING);

        knockOutQuorumNotIncludingZero();
        assertThat(state.leader(0).hostileTakeover()).isFalse();

        restoreAllNodes();
        assertThat(state.leader(0).getCurrentTokenIfLeading()).isEmpty();
    }

    @Test
    public void markNotEligibleForLeadership() throws InterruptedException {
        LeadershipToken token = state.gainLeadership(0);
        assertThat(Futures.getUnchecked(state.leader(0).isStillLeading(token))).isEqualTo(StillLeadingStatus.LEADING);

        state.leader(0).markNotEligibleForLeadership();
        assertThat(state.leader(0).stepDown()).isTrue();

        CountDownLatch waitingForLeadership = new CountDownLatch(1);
        ExecutorService exec = PTExecutors.newSingleThreadExecutor();
        Future<Void> future = exec.submit(() -> {
            waitingForLeadership.countDown();
            state.gainLeadership(0);
            return null;
        });

        waitingForLeadership.await();

        LeadershipToken token2 = state.gainLeadership(1);
        assertThat(Futures.getUnchecked(state.leader(0).isStillLeading(token)))
                .isEqualTo(StillLeadingStatus.NOT_LEADING);
        assertThat(Futures.getUnchecked(state.leader(1).isStillLeading(token2))).isEqualTo(StillLeadingStatus.LEADING);

        assertThatExceptionOfType(ExecutionException.class)
                .isThrownBy(future::get)
                .withRootCauseInstanceOf(InterruptedException.class)
                .withMessageContaining("leader no longer eligible");

        exec.shutdown();
        exec.awaitTermination(10, TimeUnit.SECONDS);
    }

    private void knockOutQuorumNotIncludingZero() {
        for (int i = 1; i < NUM_POTENTIAL_LEADERS - QUORUM_SIZE + 2; i++) {
            state.goDown(i);
        }
    }

    private void restoreAllNodes() {
        for (int i = 0; i < NUM_POTENTIAL_LEADERS; i++) {
            state.comeUp(i);
        }
    }
}
