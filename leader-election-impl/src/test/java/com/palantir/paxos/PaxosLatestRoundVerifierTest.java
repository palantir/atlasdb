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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Test;

// TODO(nziebart): these tests are mostly sanity checks, until we have better tests for {@link PaxosQuorumChecker}.
public class PaxosLatestRoundVerifierTest {

    private static final long EARLIER_ROUND = 4L;
    private static final long ROUND = 5L;
    private static final long LATER_ROUND = 6L;

    private final PaxosAcceptorNetworkClient acceptorClient = mock(PaxosAcceptorNetworkClient.class);

    private final PaxosLatestRoundVerifierImpl verifier = new PaxosLatestRoundVerifierImpl(acceptorClient);

    @Test
    public void hasQuorumIfAllNodesAgree() {
        when(acceptorClient.getLatestSequencePreparedOrAcceptedAsync())
                .thenReturn(response(ROUND, ROUND, ROUND));

        assertThat(verifier.isLatestRound(ROUND)).isEqualTo(PaxosQuorumStatus.QUORUM_AGREED);
    }

    @Test
    public void hasQuorumIfQuorumAgrees() {
        when(acceptorClient.getLatestSequencePreparedOrAcceptedAsync())
                .thenReturn(response(ROUND, ROUND, LATER_ROUND));

        assertThat(verifier.isLatestRound(ROUND)).isEqualTo(PaxosQuorumStatus.QUORUM_AGREED);
    }

    @Test
    public void hasAgreementIfLatestSequenceIsOlder() {
        when(acceptorClient.getLatestSequencePreparedOrAcceptedAsync())
                .thenReturn(response(EARLIER_ROUND, EARLIER_ROUND, LATER_ROUND));

        assertThat(verifier.isLatestRound(ROUND)).isEqualTo(PaxosQuorumStatus.QUORUM_AGREED);
    }

    @Test
    public void doesNotHaveQuorumIfQuorumFails() {
        when(acceptorClient.getLatestSequencePreparedOrAcceptedAsync())
                .thenReturn(response(ROUND));

        assertThat(verifier.isLatestRound(ROUND)).isEqualTo(PaxosQuorumStatus.NO_QUORUM);
    }

    @Test
    public void hasDisagreementIfQuorumDoesNotAgree() {
        when(acceptorClient.getLatestSequencePreparedOrAcceptedAsync())
                .thenReturn(response(ROUND, LATER_ROUND, LATER_ROUND));

        assertThat(verifier.isLatestRound(ROUND)).isEqualTo(PaxosQuorumStatus.SOME_DISAGREED);
    }

    private static ListenableFuture<PaxosResponses<PaxosLong>> response(long... latestSequences) {
        List<PaxosLong> responses = Arrays.stream(latestSequences)
                .<PaxosLong>mapToObj(ImmutablePaxosLong::of)
                .collect(Collectors.toList());
        return Futures.immediateFuture(PaxosResponses.of(2, responses));
    }

}
