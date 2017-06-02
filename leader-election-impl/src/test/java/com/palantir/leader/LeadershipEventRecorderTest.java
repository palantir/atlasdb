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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import org.junit.After;
import org.junit.Test;

import com.palantir.paxos.PaxosRoundFailureException;
import com.palantir.paxos.PaxosValue;

public class LeadershipEventRecorderTest {

    private static final String LEADER_ID = "foo";

    private final LeadershipEvents events = mock(LeadershipEvents.class);
    private final LeadershipEventRecorder recorder = new LeadershipEventRecorder(events, LEADER_ID);

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
    public void records_leadership_gained() {
        recorder.recordRound(ROUND_1_LEADING);

        verify(events).gainedLeadershipFor(ROUND_1_LEADING);
    }

    @Test
    public void records_leadership_lost() {
        recorder.recordRound(ROUND_1_LEADING);
        recorder.recordRound(ROUND_2_NOT_LEADING);

        verify(events).gainedLeadershipFor(ROUND_1_LEADING);
        verify(events).lostLeadershipFor(ROUND_1_LEADING);
    }

    @Test
    public void records_leadership_gained_after_lost() {
        recorder.recordRound(ROUND_1_LEADING);
        recorder.recordRound(ROUND_2_NOT_LEADING);
        recorder.recordRound(ROUND_3_LEADING);

        verify(events).gainedLeadershipFor(ROUND_1_LEADING);
        verify(events).lostLeadershipFor(ROUND_1_LEADING);
        verify(events).gainedLeadershipFor(ROUND_3_LEADING);
    }

    @Test
    public void records_leadership_lost_between_sequential_leadership_gains() {
        recorder.recordRound(ROUND_1_LEADING);
        recorder.recordRound(ROUND_2_LEADING);

        verify(events).gainedLeadershipFor(ROUND_1_LEADING);
        verify(events).lostLeadershipFor(ROUND_1_LEADING);
        verify(events).gainedLeadershipFor(ROUND_2_LEADING);
    }

    @Test
    public void does_not_record_duplicate_leadership_gained() {
        recorder.recordRound(ROUND_1_LEADING);
        recorder.recordRound(ROUND_1_LEADING);

        verify(events).gainedLeadershipFor(ROUND_1_LEADING);
    }

    @Test
    public void does_not_record_duplicate_leadership_lost() {
        recorder.recordRound(ROUND_1_LEADING);
        recorder.recordRound(ROUND_2_NOT_LEADING);
        recorder.recordRound(ROUND_2_NOT_LEADING);
        recorder.recordRound(ROUND_3_NOT_LEADING);

        verify(events).gainedLeadershipFor(ROUND_1_LEADING);
        verify(events).lostLeadershipFor(ROUND_1_LEADING);
    }

    @Test
    public void cannot_gain_leadership_after_losing() {
        recorder.recordRound(ROUND_1_LEADING);
        recorder.recordNotLeading(ROUND_1_LEADING);
        recorder.recordRound(ROUND_1_LEADING);

        verify(events).gainedLeadershipFor(ROUND_1_LEADING);
        verify(events).lostLeadershipFor(ROUND_1_LEADING);
    }

    @Test
    public void records_duplicate_no_quorum() {
        recorder.recordNoQuorum(ROUND_1_LEADING);
        recorder.recordNoQuorum(ROUND_1_LEADING);

        verify(events).gainedLeadershipFor(ROUND_1_LEADING);
        verify(events, times(2)).noQuorum(ROUND_1_LEADING);
    }

    @Test
    public void records_leadership_if_first_event_is_no_quorum() {
        recorder.recordNoQuorum(ROUND_1_LEADING);

        verify(events).gainedLeadershipFor(ROUND_1_LEADING);
        verify(events).noQuorum(ROUND_1_LEADING);
    }

    @Test
    public void records_leadership_if_first_event_is_not_leading() {
        recorder.recordNotLeading(ROUND_1_LEADING);

        verify(events).gainedLeadershipFor(ROUND_1_LEADING);
        verify(events).lostLeadershipFor(ROUND_1_LEADING);
    }

    @Test
    public void ignores_other_leader_ids_for_no_quorum() {
        recorder.recordNoQuorum(ROUND_1_NOT_LEADING);
    }

    @Test
    public void ignores_other_leader_ids_for_not_leading() {
        recorder.recordNotLeading(ROUND_1_NOT_LEADING);
    }

    @Test
    public void records_proposal_attempt() {
        recorder.recordProposalAttempt(5L);

        verify(events).proposedLeadershipFor(5L);
    }

    @Test
    public void records_proposal_failure() {
        PaxosRoundFailureException ex = new PaxosRoundFailureException("foo");
        recorder.recordProposalFailure(ex);

        verify(events).proposalFailure(ex);
    }

    @Test
    public void ignores_null_rounds() {
        recorder.recordRound(null);
        recorder.recordNotLeading(null);
        recorder.recordNoQuorum(null);
        recorder.recordRound(ROUND_1_LEADING);
        recorder.recordRound(null);
        recorder.recordNotLeading(null);
        recorder.recordNoQuorum(null);

        verify(events).gainedLeadershipFor(ROUND_1_LEADING);
    }

    private static PaxosValue round(long sequence, boolean leading) {
        return new PaxosValue(leading ? LEADER_ID : "other-leader", sequence, null);
    }
}
