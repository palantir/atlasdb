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
import com.palantir.paxos.Client;
import com.palantir.paxos.PaxosPromise;
import com.palantir.paxos.PaxosProposalId;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PrepareCoalescingFunctionTests {

    private static final Client CLIENT_1 = Client.of("client-1");
    private static final Client CLIENT_2 = Client.of("client-2");

    @Mock
    private BatchPaxosAcceptor remote;

    @Test
    public void canReturnBatch() {
        WithSeq<PaxosProposalId> client1seq1Id = WithSeq.of(proposalId(), 1);
        WithSeq<PaxosProposalId> client1seq2Id = WithSeq.of(proposalId(), 2);
        WithSeq<PaxosProposalId> client2seq1Id = WithSeq.of(proposalId(), 1);

        SetMultimap<Client, WithSeq<PaxosProposalId>> requests =
                ImmutableSetMultimap.<Client, WithSeq<PaxosProposalId>>builder()
                        .put(CLIENT_1, client1seq1Id)
                        .put(CLIENT_1, client1seq2Id)
                        .put(CLIENT_2, client2seq1Id)
                        .build();

        SetMultimap<Client, WithSeq<PaxosPromise>> remoteResponse =
                ImmutableSetMultimap.<Client, WithSeq<PaxosPromise>>builder()
                        .put(CLIENT_1, promiseFor(client1seq1Id))
                        .put(CLIENT_1, promiseFor(client1seq2Id))
                        .put(CLIENT_2, promiseFor(client2seq1Id))
                        .build();

        when(remote.prepare(requests)).thenReturn(remoteResponse);

        PrepareCoalescingFunction function = new PrepareCoalescingFunction(remote);
        Map<Map.Entry<Client, WithSeq<PaxosProposalId>>, PaxosPromise> result = function.apply(requests.entries());

        assertContains(
                result, CLIENT_1, client1seq1Id, promiseFor(client1seq1Id).value());
        assertContains(
                result, CLIENT_1, client1seq2Id, promiseFor(client1seq2Id).value());
        assertContains(
                result, CLIENT_2, client2seq1Id, promiseFor(client2seq1Id).value());
    }

    private static PaxosProposalId proposalId() {
        return new PaxosProposalId(new Random().nextLong(), UUID.randomUUID().toString());
    }

    private static WithSeq<PaxosPromise> promiseFor(WithSeq<PaxosProposalId> proposalIdWithSeq) {
        return proposalIdWithSeq.map(proposalId -> PaxosPromise.accept(proposalId, null, null));
    }

    private static <A, B, C> void assertContains(Map<Map.Entry<A, B>, C> map, A first, B second, C third) {
        assertThat(contains(map, first, second, third))
                .as("Map contains desired entry")
                .isTrue();
    }

    private static <A, B, C> boolean contains(Map<Map.Entry<A, B>, C> map, A first, B second, C third) {
        return map.entrySet().stream()
                .anyMatch(entry -> entry.getKey().equals(entry(first, second))
                        && entry.getValue().equals(third));
    }
}
