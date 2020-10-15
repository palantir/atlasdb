/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.paxos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
import com.palantir.paxos.BooleanPaxosResponse;
import com.palantir.paxos.Client;
import com.palantir.paxos.PaxosProposal;
import com.palantir.paxos.PaxosProposalId;
import com.palantir.paxos.PaxosValue;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AcceptCoalescingFunctionTests {

    private static final Client CLIENT_1 = Client.of("client-1");
    private static final Client CLIENT_2 = Client.of("client-2");
    private static final BooleanPaxosResponse SUCCESS = new BooleanPaxosResponse(true);
    private static final BooleanPaxosResponse FAILURE = new BooleanPaxosResponse(false);

    @Mock
    private BatchPaxosAcceptor remote;

    @Test
    public void canProcessBatch() {
        PaxosProposal client1seq1Proposal = proposal(1);
        PaxosProposal client1seq2Proposal = proposal(2);
        PaxosProposal client2seq1Proposal = proposal(1);

        SetMultimap<Client, PaxosProposal> requests = ImmutableSetMultimap.<Client, PaxosProposal>builder()
                .put(CLIENT_1, client1seq1Proposal)
                .put(CLIENT_1, client1seq2Proposal)
                .put(CLIENT_2, client2seq1Proposal)
                .build();

        SetMultimap<Client, WithSeq<BooleanPaxosResponse>> remoteResponse =
                ImmutableSetMultimap.<Client, WithSeq<BooleanPaxosResponse>>builder()
                        .put(CLIENT_1, success(client1seq1Proposal))
                        .put(CLIENT_1, failure(client1seq2Proposal))
                        .put(CLIENT_2, success(client2seq1Proposal))
                        .build();


        when(remote.accept(requests)).thenReturn(remoteResponse);

        AcceptCoalescingFunction function = new AcceptCoalescingFunction(remote);
        Map<Map.Entry<Client, PaxosProposal>, BooleanPaxosResponse> result = function.apply(requests.entries());

        assertThat(result)
                .containsEntry(entry(CLIENT_1, client1seq1Proposal), success(client1seq1Proposal).value())
                .containsEntry(entry(CLIENT_1, client1seq2Proposal), failure(client1seq2Proposal).value())
                .containsEntry(entry(CLIENT_2, client2seq1Proposal), success(client2seq1Proposal).value());
    }

    private static PaxosProposal proposal(long round) {
        PaxosProposalId proposalId = new PaxosProposalId(new Random().nextLong(), UUID.randomUUID().toString());
        PaxosValue value = new PaxosValue(UUID.randomUUID().toString(), round, null);
        return new PaxosProposal(proposalId, value);
    }

    private static WithSeq<BooleanPaxosResponse> success(PaxosProposal proposal) {
        return WithSeq.of(SUCCESS, proposal.getValue().getRound());
    }

    private static WithSeq<BooleanPaxosResponse> failure(PaxosProposal proposal) {
        return WithSeq.of(FAILURE, proposal.getValue().getRound());
    }
}
