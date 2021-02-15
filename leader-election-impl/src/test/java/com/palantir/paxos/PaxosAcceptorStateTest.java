/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

import org.junit.Test;

public class PaxosAcceptorStateTest {
    private static final PaxosProposalId LAST_PROMISED = new PaxosProposalId(15, "id");
    private static final PaxosProposalId LAST_ACCEPTED = new PaxosProposalId(13, "otherId");
    private static final PaxosValue VALUE = PaxosStateLogTestUtils.valueForRound(13);

    @Test
    public void hydrationEqualityTest() {
        PaxosAcceptorState state = PaxosAcceptorState.newState(LAST_PROMISED);
        state = state.withState(LAST_PROMISED, LAST_ACCEPTED, VALUE);
        PaxosAcceptorState hydrated = PaxosAcceptorState.BYTES_HYDRATOR.hydrateFromBytes(state.persistToBytes());
        assertThat(state.equalsIgnoringVersion(hydrated)).isTrue();
    }

    @Test
    public void equalityTest() {
        PaxosAcceptorState state = PaxosAcceptorState.newState(LAST_PROMISED);
        PaxosAcceptorState otherState = PaxosAcceptorState.newState(LAST_PROMISED);

        state = state.withState(LAST_PROMISED, LAST_ACCEPTED, VALUE);
        otherState = otherState.withState(LAST_PROMISED, LAST_ACCEPTED, VALUE);
        assertThat(state.equalsIgnoringVersion(otherState)).isTrue();
    }

    @Test
    public void inequalityForPromised() {
        PaxosAcceptorState state = PaxosAcceptorState.newState(LAST_PROMISED);
        PaxosAcceptorState otherState = PaxosAcceptorState.newState(new PaxosProposalId(16, "id"));
        assertThat(state.equalsIgnoringVersion(otherState)).isFalse();
    }

    @Test
    public void inequalityForAccepted() {
        PaxosAcceptorState state = PaxosAcceptorState.newState(LAST_PROMISED);
        PaxosAcceptorState otherState = PaxosAcceptorState.newState(LAST_PROMISED);

        state = state.withState(LAST_PROMISED, LAST_ACCEPTED, VALUE);
        otherState = otherState.withState(LAST_PROMISED, new PaxosProposalId(13, "other"), VALUE);
        assertThat(state.equalsIgnoringVersion(otherState)).isFalse();
    }

    @Test
    public void inequalityForPaxosValue() {
        PaxosAcceptorState state = PaxosAcceptorState.newState(LAST_PROMISED);
        PaxosAcceptorState otherState = PaxosAcceptorState.newState(LAST_PROMISED);

        state = state.withState(LAST_PROMISED, LAST_ACCEPTED, VALUE);
        otherState = otherState.withState(LAST_PROMISED, LAST_ACCEPTED, PaxosStateLogTestUtils.valueForRound(12));
        assertThat(state.equalsIgnoringVersion(otherState)).isFalse();
    }
}
