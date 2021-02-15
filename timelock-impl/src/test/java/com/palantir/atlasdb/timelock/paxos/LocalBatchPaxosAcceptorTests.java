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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
import com.palantir.paxos.BooleanPaxosResponse;
import com.palantir.paxos.Client;
import com.palantir.paxos.PaxosPromise;
import com.palantir.paxos.PaxosProposal;
import com.palantir.paxos.PaxosProposalId;
import com.palantir.paxos.PaxosValue;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("unchecked")
@RunWith(MockitoJUnitRunner.class)
public class LocalBatchPaxosAcceptorTests {

    private static final Client CLIENT_1 = Client.of("client1");
    private static final Client CLIENT_2 = Client.of("client2");
    private static final PaxosProposalId PROPOSAL_ID_1 = proposalId();
    private static final PaxosProposalId PROPOSAL_ID_2 = proposalId();

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private LocalPaxosComponents components;

    @Mock
    private AcceptorCache cache;

    private LocalBatchPaxosAcceptor resource;

    @Before
    public void before() {
        resource = new LocalBatchPaxosAcceptor(components, cache);
    }

    @Test
    public void weProxyPrepareRequests() {
        when(components.acceptor(CLIENT_1).prepare(1, PROPOSAL_ID_1)).thenReturn(promise(PROPOSAL_ID_1));
        when(components.acceptor(CLIENT_2).prepare(1, PROPOSAL_ID_1)).thenReturn(promise(PROPOSAL_ID_1));
        when(components.acceptor(CLIENT_2).prepare(2, PROPOSAL_ID_2)).thenReturn(promise(PROPOSAL_ID_2));

        when(components.acceptor(CLIENT_1).getLatestSequencePreparedOrAccepted())
                .thenReturn(1L);
        when(components.acceptor(CLIENT_2).getLatestSequencePreparedOrAccepted())
                .thenReturn(2L);

        SetMultimap<Client, WithSeq<PaxosProposalId>> request =
                ImmutableSetMultimap.<Client, WithSeq<PaxosProposalId>>builder()
                        .put(CLIENT_1, WithSeq.of(PROPOSAL_ID_1, 1))
                        .putAll(CLIENT_2, WithSeq.of(PROPOSAL_ID_1, 1), WithSeq.of(PROPOSAL_ID_2, 2))
                        .build();

        SetMultimap<Client, WithSeq<PaxosPromise>> expected =
                ImmutableSetMultimap.<Client, WithSeq<PaxosPromise>>builder()
                        .put(CLIENT_1, WithSeq.of(promise(PROPOSAL_ID_1), 1))
                        .putAll(CLIENT_2, WithSeq.of(promise(PROPOSAL_ID_1), 1), WithSeq.of(promise(PROPOSAL_ID_2), 2))
                        .build();

        assertThat(resource.prepare(request)).isEqualTo(expected);

        verify(cache).updateSequenceNumbers(ImmutableSet.of(WithSeq.of(CLIENT_1, 1), WithSeq.of(CLIENT_2, 2)));
    }

    @Test
    public void weProxyAcceptRequests() {
        PaxosProposal proposal1 = proposal(PROPOSAL_ID_1, 1);
        PaxosProposal proposal2 = proposal(PROPOSAL_ID_2, 2);

        when(components.acceptor(CLIENT_1).accept(1, proposal1)).thenReturn(success());
        when(components.acceptor(CLIENT_2).accept(1, proposal1)).thenReturn(success());
        when(components.acceptor(CLIENT_2).accept(2, proposal2)).thenReturn(success());

        when(components.acceptor(CLIENT_1).getLatestSequencePreparedOrAccepted())
                .thenReturn(1L);
        when(components.acceptor(CLIENT_2).getLatestSequencePreparedOrAccepted())
                .thenReturn(2L);

        SetMultimap<Client, PaxosProposal> request = ImmutableSetMultimap.<Client, PaxosProposal>builder()
                .put(CLIENT_1, proposal1)
                .put(CLIENT_2, proposal1)
                .put(CLIENT_2, proposal2)
                .build();

        SetMultimap<Client, WithSeq<BooleanPaxosResponse>> expected =
                ImmutableSetMultimap.<Client, WithSeq<BooleanPaxosResponse>>builder()
                        .put(CLIENT_1, WithSeq.of(success(), 1))
                        .put(CLIENT_2, WithSeq.of(success(), 1))
                        .put(CLIENT_2, WithSeq.of(success(), 2))
                        .build();

        assertThat(resource.accept(request)).isEqualTo(expected);

        verify(cache).updateSequenceNumbers(ImmutableSet.of(WithSeq.of(CLIENT_1, 1), WithSeq.of(CLIENT_2, 2)));
    }

    @Test
    public void cachedEndpointDelegatesToCache() throws InvalidAcceptorCacheKeyException {
        AcceptorCacheKey cacheKey = AcceptorCacheKey.newCacheKey();
        AcceptorCacheDigest digest = digest();
        when(cache.updatesSinceCacheKey(cacheKey)).thenReturn(Optional.of(digest));

        assertThat(resource.latestSequencesPreparedOrAcceptedCached(cacheKey)).contains(digest);
    }

    @Test
    public void returnsEverythingIfCacheKeyIsNotProvided() throws InvalidAcceptorCacheKeyException {
        when(components.acceptor(CLIENT_1).getLatestSequencePreparedOrAccepted())
                .thenReturn(1L);
        when(components.acceptor(CLIENT_2).getLatestSequencePreparedOrAccepted())
                .thenReturn(2L);

        AcceptorCacheDigest digest = ImmutableAcceptorCacheDigest.builder()
                .newCacheKey(AcceptorCacheKey.newCacheKey())
                .putUpdates(CLIENT_1, 1L)
                .putUpdates(CLIENT_2, 2L)
                .cacheTimestamp(5)
                .build();

        when(cache.getAllUpdates()).thenReturn(digest);

        assertThat(resource.latestSequencesPreparedOrAccepted(Optional.empty(), ImmutableSet.of(CLIENT_1, CLIENT_2)))
                .isEqualTo(digest);

        verify(cache).updateSequenceNumbers(ImmutableSet.of(WithSeq.of(CLIENT_1, 1), WithSeq.of(CLIENT_2, 2)));
    }

    @Test
    public void returnsOnlyUpdatesIfCacheKeyIsProvidedButPrimesCache() throws InvalidAcceptorCacheKeyException {
        when(components.acceptor(CLIENT_1).getLatestSequencePreparedOrAccepted())
                .thenReturn(1L);
        when(components.acceptor(CLIENT_2).getLatestSequencePreparedOrAccepted())
                .thenReturn(2L);

        AcceptorCacheKey cacheKey = AcceptorCacheKey.newCacheKey();

        AcceptorCacheDigest digest = ImmutableAcceptorCacheDigest.builder()
                .newCacheKey(AcceptorCacheKey.newCacheKey())
                .putUpdates(CLIENT_2, 2L)
                .cacheTimestamp(5)
                .build();

        when(cache.updatesSinceCacheKey(cacheKey)).thenReturn(Optional.of(digest));

        assertThat(resource.latestSequencesPreparedOrAccepted(
                        Optional.of(cacheKey), ImmutableSet.of(CLIENT_1, CLIENT_2)))
                .isEqualTo(digest);

        verify(cache).updateSequenceNumbers(ImmutableSet.of(WithSeq.of(CLIENT_1, 1), WithSeq.of(CLIENT_2, 2)));
    }

    private static PaxosProposalId proposalId() {
        return new PaxosProposalId(new Random().nextLong(), UUID.randomUUID().toString());
    }

    private static PaxosPromise promise(PaxosProposalId proposalId) {
        return PaxosPromise.accept(proposalId, null, null);
    }

    private static BooleanPaxosResponse success() {
        return new BooleanPaxosResponse(true);
    }

    private static PaxosProposal proposal(PaxosProposalId proposalId, long seq) {
        return new PaxosProposal(proposalId, paxosValue(seq));
    }

    private static PaxosValue paxosValue(long seq) {
        return new PaxosValue(UUID.randomUUID().toString(), seq, null);
    }

    private static AcceptorCacheDigest digest() {
        return ImmutableAcceptorCacheDigest.builder()
                .newCacheKey(AcceptorCacheKey.newCacheKey())
                .putUpdates(CLIENT_1, 50L)
                .cacheTimestamp(1L)
                .build();
    }
}
