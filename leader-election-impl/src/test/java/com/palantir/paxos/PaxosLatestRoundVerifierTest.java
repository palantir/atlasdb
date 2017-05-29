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

package com.palantir.paxos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

// TODO(nziebart): these tests are mostly sanity checks, until we have better tests for {@link PaxosQuorumChecker}.
public class PaxosLatestRoundVerifierTest {

    private static final long ROUND = 5L;
    private static final long LATER_ROUND = 6L;

    private final PaxosAcceptor acceptor1 = mock(PaxosAcceptor.class);
    private final PaxosAcceptor acceptor2 = mock(PaxosAcceptor.class);
    private final PaxosAcceptor acceptor3 = mock(PaxosAcceptor.class);

    private final List<PaxosAcceptor> acceptors = ImmutableList.of(
            acceptor1,
            acceptor2,
            acceptor3);

    private final PaxosLatestRoundVerifierImpl verifier = new PaxosLatestRoundVerifierImpl(acceptors, 2);

    @Test
    public void has_quorum_if_all_nodes_agree() {
        when(acceptor1.getLatestSequencePreparedOrAccepted()).thenReturn(ROUND);
        when(acceptor2.getLatestSequencePreparedOrAccepted()).thenReturn(ROUND);
        when(acceptor3.getLatestSequencePreparedOrAccepted()).thenReturn(ROUND);

        assertThat(verifier.isLatestRound(ROUND)).isEqualTo(PaxosQuorumResult.QUORUM_AGREED);
    }

    @Test
    public void has_quorum_if_quorum_agrees() {
        when(acceptor1.getLatestSequencePreparedOrAccepted()).thenReturn(ROUND);
        when(acceptor2.getLatestSequencePreparedOrAccepted()).thenReturn(ROUND);
        when(acceptor3.getLatestSequencePreparedOrAccepted()).thenReturn(LATER_ROUND);

        assertThat(verifier.isLatestRound(ROUND)).isEqualTo(PaxosQuorumResult.QUORUM_AGREED);
    }

    @Test
    public void does_not_have_quorum_if_nodes_fail() {
        when(acceptor1.getLatestSequencePreparedOrAccepted()).thenThrow(new RuntimeException("foo"));
        when(acceptor2.getLatestSequencePreparedOrAccepted()).thenThrow(new RuntimeException("foo"));
        when(acceptor3.getLatestSequencePreparedOrAccepted()).thenReturn(ROUND);

        assertThat(verifier.isLatestRound(ROUND)).isEqualTo(PaxosQuorumResult.NO_QUORUM);
    }

    @Test
    public void has_disagreement_if_quorum_does_not_agree() {
        when(acceptor1.getLatestSequencePreparedOrAccepted()).thenReturn(ROUND);
        when(acceptor2.getLatestSequencePreparedOrAccepted()).thenReturn(LATER_ROUND);
        when(acceptor3.getLatestSequencePreparedOrAccepted()).thenReturn(LATER_ROUND);

        assertThat(verifier.isLatestRound(ROUND)).isEqualTo(PaxosQuorumResult.SOME_DISAGREED);
    }

}
