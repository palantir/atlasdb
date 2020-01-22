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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.palantir.paxos.PaxosRoundFailureException;
import com.palantir.paxos.PaxosValue;
import java.util.Optional;
import org.junit.After;
import org.junit.Test;

public class LeadershipEventRecorderTest {

    private static final String LEADER_ID = "foo";

    private final LeadershipEvents events = mock(LeadershipEvents.class);
    private final LeadershipObserver observer = mock(LeadershipObserver.class);
    private final PaxosLeadershipEventRecorder recorder =
            new PaxosLeadershipEventRecorder(events, LEADER_ID, Optional.of(observer));

    private static final PaxosValue ROUND_1_LEADING = round(1, true);
    private static final PaxosValue ROUND_2_LEADING = round(2, true);
    private static final PaxosValue ROUND_3_LEADING = round(3, true);
    private static final PaxosValue ROUND_1_NOT_LEADING = round(1, false);
    private static final PaxosValue ROUND_2_NOT_LEADING = round(2, false);
    private static final PaxosValue ROUND_3_NOT_LEADING = round(3, false);

    @After
    public void verifyNoMoreEvents() {
        verifyNoMoreInteractions(events);
    }

    @Test
    public void recordsLeadershipGained() {
        recorder.recordRound(ROUND_1_LEADING);

        verify(events).gainedLeadershipFor(ROUND_1_LEADING);
    }

    @Test
    public void recordsLeadershipLost() {
        recorder.recordRound(ROUND_1_LEADING);
        recorder.recordRound(ROUND_2_NOT_LEADING);

        verify(events).gainedLeadershipFor(ROUND_1_LEADING);
        verify(events).lostLeadershipFor(ROUND_1_LEADING);
    }

    @Test
    public void reacordsLeadershipGainedAfterLost() {
        recorder.recordRound(ROUND_1_LEADING);
        recorder.recordRound(ROUND_2_NOT_LEADING);
        recorder.recordRound(ROUND_3_LEADING);

        verify(events).gainedLeadershipFor(ROUND_1_LEADING);
        verify(events).lostLeadershipFor(ROUND_1_LEADING);
        verify(events).gainedLeadershipFor(ROUND_3_LEADING);
    }

    @Test
    public void recordsLeadershipLostBetweenSequentialLeadershipGained() {
        recorder.recordRound(ROUND_1_LEADING);
        recorder.recordRound(ROUND_2_LEADING);

        verify(events).gainedLeadershipFor(ROUND_1_LEADING);
        verify(events).lostLeadershipFor(ROUND_1_LEADING);
        verify(events).gainedLeadershipFor(ROUND_2_LEADING);
    }

    @Test
    public void doesNotRecordDuplicateLeadershipGained() {
        recorder.recordRound(ROUND_1_LEADING);
        recorder.recordRound(ROUND_1_LEADING);

        verify(events).gainedLeadershipFor(ROUND_1_LEADING);
    }

    @Test
    public void doesNotRecordDuplicateLeadershipLost() {
        recorder.recordRound(ROUND_1_LEADING);
        recorder.recordRound(ROUND_2_NOT_LEADING);
        recorder.recordRound(ROUND_2_NOT_LEADING);
        recorder.recordRound(ROUND_3_NOT_LEADING);

        verify(events).gainedLeadershipFor(ROUND_1_LEADING);
        verify(events).lostLeadershipFor(ROUND_1_LEADING);
    }

    @Test
    public void cannotGainLeadershipAfterLosing() {
        recorder.recordRound(ROUND_1_LEADING);
        recorder.recordNotLeading(ROUND_1_LEADING);
        recorder.recordRound(ROUND_1_LEADING);

        verify(events).gainedLeadershipFor(ROUND_1_LEADING);
        verify(events).lostLeadershipFor(ROUND_1_LEADING);
    }

    @Test
    public void cannotGainLeadershipAfterLosingWithOutOfOrderRounds() {
        recorder.recordRound(ROUND_1_LEADING);
        recorder.recordRound(ROUND_3_NOT_LEADING);
        recorder.recordRound(ROUND_2_LEADING);

        verify(events).gainedLeadershipFor(ROUND_1_LEADING);
        verify(events).lostLeadershipFor(ROUND_1_LEADING);
    }

    @Test
    public void recordsDuplicateNoQuorum() {
        recorder.recordRound(ROUND_1_LEADING);
        recorder.recordNoQuorum(ROUND_1_LEADING);
        recorder.recordNoQuorum(ROUND_1_LEADING);

        verify(events).gainedLeadershipFor(ROUND_1_LEADING);
        verify(events, times(2)).noQuorum(ROUND_1_LEADING);
    }

    @Test
    public void ignoresOtherLeaderIdsForNoQuorum() {
        recorder.recordNoQuorum(ROUND_1_NOT_LEADING);
    }

    @Test
    public void ignoresOtherLeaderIdsForNotLeading() {
        recorder.recordNotLeading(ROUND_1_NOT_LEADING);
    }

    @Test
    public void recordsProposalAttempt() {
        recorder.recordProposalAttempt(5L);

        verify(events).proposedLeadershipFor(5L);
    }

    @Test
    public void recordsProposalFailure() {
        PaxosRoundFailureException ex = new PaxosRoundFailureException("foo");
        recorder.recordProposalFailure(ex);

        verify(events).proposalFailure(ex);
    }

    @Test
    public void recordsLeaderPingFailure() {
        Throwable error = new RuntimeException("foo");
        recorder.recordLeaderPingFailure(error);

        verify(events).leaderPingFailure(error);
    }

    @Test
    public void recordsLeaderPingTimeout() {
        recorder.recordLeaderPingTimeout();

        verify(events).leaderPingTimeout();
    }

    @Test
    public void recordsLeaderPingReturnedFalse() {
        recorder.recordLeaderPingReturnedFalse();

        verify(events).leaderPingReturnedFalse();
    }

    @Test
    public void ignoresNullRounds() {
        recorder.recordRound(null);
        recorder.recordNotLeading(null);
        recorder.recordNoQuorum(null);
        recorder.recordRound(ROUND_1_LEADING);
        recorder.recordRound(null);
        recorder.recordNotLeading(null);
        recorder.recordNoQuorum(null);

        verify(events).gainedLeadershipFor(ROUND_1_LEADING);
    }

    @Test
    public void notifiesObserverIfGainedLeadership() {
        recorder.recordRound(ROUND_1_LEADING);

        verify(events).gainedLeadershipFor(ROUND_1_LEADING);
        verify(observer).gainedLeadership();
    }

    @Test
    public void notifiesObserverIfLostLeadership() {
        recorder.recordRound(ROUND_1_LEADING);
        recorder.recordRound(ROUND_2_NOT_LEADING);

        verify(events).gainedLeadershipFor(ROUND_1_LEADING);
        verify(events).lostLeadershipFor(ROUND_1_LEADING);

        verify(observer).gainedLeadership();
        verify(observer).lostLeadership();
    }

    private static PaxosValue round(long sequence, boolean leading) {
        return new PaxosValue(leading ? LEADER_ID : "other-leader", sequence, null);
    }
}
