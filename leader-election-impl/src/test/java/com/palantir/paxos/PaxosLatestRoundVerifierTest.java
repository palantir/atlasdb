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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

// TODO(nziebart): these tests are mostly sanity checks, until we have better tests for {@link PaxosQuorumChecker}.
public class PaxosLatestRoundVerifierTest {

    private static final long EARLIER_ROUND = 4L;
    private static final long ROUND = 5L;
    private static final long LATER_ROUND = 6L;

    private final PaxosAcceptor acceptor1 = mock(PaxosAcceptor.class);
    private final PaxosAcceptor acceptor2 = mock(PaxosAcceptor.class);
    private final PaxosAcceptor acceptor3 = mock(PaxosAcceptor.class);

    private final List<PaxosAcceptor> acceptors = ImmutableList.of(
            acceptor1,
            acceptor2,
            acceptor3);

    private final Supplier<Boolean> onlyLogOnQuorumFailure = () -> false;

    private final PaxosLatestRoundVerifierImpl verifier = new PaxosLatestRoundVerifierImpl(acceptors, 2,
            Executors.newCachedThreadPool(), onlyLogOnQuorumFailure);

    @Test
    public void hasQuorumIfAllNodesAgree() {
        when(acceptor1.getLatestSequencePreparedOrAccepted()).thenReturn(ROUND);
        when(acceptor2.getLatestSequencePreparedOrAccepted()).thenReturn(ROUND);
        when(acceptor3.getLatestSequencePreparedOrAccepted()).thenReturn(ROUND);

        assertThat(verifier.isLatestRound(ROUND)).isEqualTo(PaxosQuorumStatus.QUORUM_AGREED);
    }

    @Test
    public void hasQuorumIfQuorumAgrees() {
        when(acceptor1.getLatestSequencePreparedOrAccepted()).thenReturn(ROUND);
        when(acceptor2.getLatestSequencePreparedOrAccepted()).thenReturn(ROUND);
        when(acceptor3.getLatestSequencePreparedOrAccepted()).thenReturn(LATER_ROUND);

        assertThat(verifier.isLatestRound(ROUND)).isEqualTo(PaxosQuorumStatus.QUORUM_AGREED);
    }

    @Test
    public void hasAgreementIfLatestSequenceIsOlder() {
        when(acceptor1.getLatestSequencePreparedOrAccepted()).thenReturn(EARLIER_ROUND);
        when(acceptor2.getLatestSequencePreparedOrAccepted()).thenReturn(EARLIER_ROUND);
        when(acceptor3.getLatestSequencePreparedOrAccepted()).thenReturn(LATER_ROUND);

        assertThat(verifier.isLatestRound(ROUND)).isEqualTo(PaxosQuorumStatus.QUORUM_AGREED);
    }

    @Test
    public void doesNotHaveQuorumIfQuorumFails() {
        when(acceptor1.getLatestSequencePreparedOrAccepted()).thenThrow(new RuntimeException("foo"));
        when(acceptor2.getLatestSequencePreparedOrAccepted()).thenThrow(new RuntimeException("foo"));
        when(acceptor3.getLatestSequencePreparedOrAccepted()).thenReturn(ROUND);

        assertThat(verifier.isLatestRound(ROUND)).isEqualTo(PaxosQuorumStatus.NO_QUORUM);
    }

    @Test
    public void hasDisagreementIfQuorumDoesNotAgree() {
        when(acceptor1.getLatestSequencePreparedOrAccepted()).thenReturn(ROUND);
        when(acceptor2.getLatestSequencePreparedOrAccepted()).thenReturn(LATER_ROUND);
        when(acceptor3.getLatestSequencePreparedOrAccepted()).thenReturn(LATER_ROUND);

        assertThat(verifier.isLatestRound(ROUND)).isEqualTo(PaxosQuorumStatus.SOME_DISAGREED);
    }

}
