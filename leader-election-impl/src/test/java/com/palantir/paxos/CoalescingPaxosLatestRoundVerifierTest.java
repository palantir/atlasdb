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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;

public class CoalescingPaxosLatestRoundVerifierTest {

    private static final long ROUND_ONE = 1L;
    private static final long ROUND_TWO = 2L;

    private PaxosLatestRoundVerifier delegate = mock(PaxosLatestRoundVerifier.class);
    private CoalescingPaxosLatestRoundVerifier verifier = new CoalescingPaxosLatestRoundVerifier(delegate);

    @Test
    public void returnsDelegateValue() {
        PaxosQuorumStatus expected1 = PaxosQuorumStatus.SOME_DISAGREED;
        when(delegate.isLatestRound(ROUND_ONE)).thenReturn(expected1);
        assertThat(verifier.isLatestRound(ROUND_ONE)).isEqualTo(expected1);

        PaxosQuorumStatus expected2 = PaxosQuorumStatus.QUORUM_AGREED;
        when(delegate.isLatestRound(ROUND_ONE)).thenReturn(expected2);
        assertThat(verifier.isLatestRound(ROUND_ONE)).isEqualTo(expected2);
    }

    @Test
    public void usesCorrectDelegateForDifferentRounds() {
        PaxosQuorumStatus round1Status = PaxosQuorumStatus.SOME_DISAGREED;
        PaxosQuorumStatus round2Status = PaxosQuorumStatus.NO_QUORUM;

        when(delegate.isLatestRound(ROUND_ONE)).thenReturn(round1Status);
        when(delegate.isLatestRound(ROUND_TWO)).thenReturn(round2Status);

        assertThat(verifier.isLatestRound(ROUND_ONE)).isEqualTo(round1Status);
        assertThat(verifier.isLatestRound(ROUND_TWO)).isEqualTo(round2Status);
    }

    @Test
    public void propagatesExceptions() {
        RuntimeException expected = new RuntimeException("foo");

        when(delegate.isLatestRound(ROUND_ONE)).thenThrow(expected);

        assertThatThrownBy(() -> verifier.isLatestRound(ROUND_ONE)).isEqualTo(expected);
    }
}
