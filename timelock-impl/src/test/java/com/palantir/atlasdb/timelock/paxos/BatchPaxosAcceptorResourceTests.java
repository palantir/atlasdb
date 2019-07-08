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

import static com.palantir.conjure.java.api.testing.Assertions.assertThatServiceExceptionThrownBy;

import java.util.Optional;
import java.util.Random;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
import com.palantir.logsafe.SafeArg;
import com.palantir.paxos.BooleanPaxosResponse;
import com.palantir.paxos.PaxosPromise;
import com.palantir.paxos.PaxosProposal;
import com.palantir.paxos.PaxosProposalId;
import com.palantir.paxos.PaxosValue;

@SuppressWarnings("unchecked")
@RunWith(MockitoJUnitRunner.class)
public class BatchPaxosAcceptorResourceTests {

    private static final Client CLIENT_1 = Client.of("client1");
    private static final Client CLIENT_2 = Client.of("client2");
    private static final PaxosProposalId PROPOSAL_ID_1 = proposalId();
    private static final PaxosProposalId PROPOSAL_ID_2 = proposalId();

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private PaxosComponents components;

    @Mock
    private AcceptorCache cache;

    private BatchPaxosAcceptorResource resource;

    @Before
    public void before() {
        resource = new BatchPaxosAcceptorResource(components, cache);
    }

    @Test
    public void weProxyPrepareRequests() {
        when(components.acceptor(CLIENT_1).prepare(1, PROPOSAL_ID_1)).thenReturn(promise(PROPOSAL_ID_1));
        when(components.acceptor(CLIENT_2).prepare(1, PROPOSAL_ID_1)).thenReturn(promise(PROPOSAL_ID_1));
        when(components.acceptor(CLIENT_2).prepare(2, PROPOSAL_ID_2)).thenReturn(promise(PROPOSAL_ID_2));

        when(components.acceptor(CLIENT_1).getLatestSequencePreparedOrAccepted()).thenReturn(1L);
        when(components.acceptor(CLIENT_2).getLatestSequencePreparedOrAccepted()).thenReturn(2L);

        SetMultimap<Client, WithSeq<PaxosProposalId>> request = ImmutableSetMultimap
                .<Client, WithSeq<PaxosProposalId>>builder()
                .put(CLIENT_1, WithSeq.of(1, PROPOSAL_ID_1))
                .putAll(CLIENT_2, WithSeq.of(1, PROPOSAL_ID_1), WithSeq.of(2, PROPOSAL_ID_2))
                .build();

        SetMultimap<Client, WithSeq<PaxosPromise>> expected = ImmutableSetMultimap
                .<Client, WithSeq<PaxosPromise>>builder()
                .put(CLIENT_1, WithSeq.of(1, promise(PROPOSAL_ID_1)))
                .putAll(CLIENT_2, WithSeq.of(1, promise(PROPOSAL_ID_1)), WithSeq.of(2, promise(PROPOSAL_ID_2)))
                .build();

        assertThat(resource.prepare(request))
                .isEqualTo(expected);

        verify(cache).updateSequenceNumbers(ImmutableSet.of(
                WithSeq.of(1, CLIENT_1),
                WithSeq.of(2, CLIENT_2)));
    }

    @Test
    public void weProxyAcceptRequests() {
        PaxosProposal proposal1 = proposal(PROPOSAL_ID_1);
        PaxosProposal proposal2 = proposal(PROPOSAL_ID_2);

        when(components.acceptor(CLIENT_1).accept(1, proposal1)).thenReturn(success());
        when(components.acceptor(CLIENT_2).accept(1, proposal1)).thenReturn(success());
        when(components.acceptor(CLIENT_2).accept(2, proposal2)).thenReturn(success());

        when(components.acceptor(CLIENT_1).getLatestSequencePreparedOrAccepted()).thenReturn(1L);
        when(components.acceptor(CLIENT_2).getLatestSequencePreparedOrAccepted()).thenReturn(2L);

        SetMultimap<Client, WithSeq<PaxosProposal>> request = ImmutableSetMultimap
                .<Client, WithSeq<PaxosProposal>>builder()
                .put(CLIENT_1, WithSeq.of(1, proposal1))
                .put(CLIENT_2, WithSeq.of(1, proposal1))
                .put(CLIENT_2, WithSeq.of(2, proposal2))
                .build();

        SetMultimap<Client, WithSeq<BooleanPaxosResponse>> expected = ImmutableSetMultimap
                .<Client, WithSeq<BooleanPaxosResponse>>builder()
                .put(CLIENT_1, WithSeq.of(1, success()))
                .put(CLIENT_2, WithSeq.of(1, success()))
                .put(CLIENT_2, WithSeq.of(2, success()))
                .build();

        assertThat(resource.accept(request))
                .isEqualTo(expected);

        verify(cache).updateSequenceNumbers(ImmutableSet.of(
                WithSeq.of(1, CLIENT_1),
                WithSeq.of(2, CLIENT_2)));
    }

    @Test
    public void throwsConjureRuntime404WhenCacheKeyIsNotFound() throws InvalidAcceptorCacheKeyException {
        AcceptorCacheKey cacheKey = AcceptorCacheKey.of(UUID.randomUUID());
        when(cache.updatesSinceCacheKey(cacheKey))
                .thenThrow(new InvalidAcceptorCacheKeyException(cacheKey));

        assertThatServiceExceptionThrownBy(() -> resource.latestSequencesPreparedOrAcceptedCached(cacheKey))
                .hasType(BatchPaxosAcceptorResource.CACHE_KEY_NOT_FOUND)
                .hasArgs(SafeArg.of("cacheKey", cacheKey));
    }

    @Test
    public void cachedEndpointDelegatesToCache() throws InvalidAcceptorCacheKeyException {
        AcceptorCacheKey cacheKey = AcceptorCacheKey.of(UUID.randomUUID());
        AcceptorCacheDigest digest = digest();
        when(cache.updatesSinceCacheKey(cacheKey))
                .thenReturn(Optional.of(digest));

        assertThat(resource.latestSequencesPreparedOrAcceptedCached(cacheKey))
                .contains(digest);
    }

    @Test
    public void returnsEverythingIfCacheKeyIsNotProvided() {
        when(components.acceptor(CLIENT_1).getLatestSequencePreparedOrAccepted()).thenReturn(1L);
        when(components.acceptor(CLIENT_2).getLatestSequencePreparedOrAccepted()).thenReturn(2L);

        AcceptorCacheDigest digest = ImmutableAcceptorCacheDigest.builder()
                .newCacheKey(UUID.randomUUID())
                .putUpdates(CLIENT_1, 1L)
                .putUpdates(CLIENT_2, 2L)
                .build();

        when(cache.getAllUpdates()).thenReturn(digest);

        assertThat(resource.latestSequencesPreparedOrAccepted(Optional.empty(), ImmutableSet.of(CLIENT_1, CLIENT_2)))
                .isEqualTo(digest);

        verify(cache).updateSequenceNumbers(ImmutableSet.of(
                WithSeq.of(1, CLIENT_1),
                WithSeq.of(2, CLIENT_2)));
    }

    @Test
    public void returnsOnlyUpdatesIfCacheKeyIsProvidedButPrimesCache() throws InvalidAcceptorCacheKeyException {
        when(components.acceptor(CLIENT_1).getLatestSequencePreparedOrAccepted()).thenReturn(1L);
        when(components.acceptor(CLIENT_2).getLatestSequencePreparedOrAccepted()).thenReturn(2L);

        AcceptorCacheKey cacheKey = AcceptorCacheKey.of(UUID.randomUUID());

        AcceptorCacheDigest digest = ImmutableAcceptorCacheDigest.builder()
                .newCacheKey(UUID.randomUUID())
                .putUpdates(CLIENT_2, 2L)
                .build();

        when(cache.updatesSinceCacheKey(cacheKey))
                .thenReturn(Optional.of(digest));

        assertThat(resource.latestSequencesPreparedOrAccepted(
                Optional.of(cacheKey),
                ImmutableSet.of(CLIENT_1, CLIENT_2)))
                .isEqualTo(digest);

        verify(cache).updateSequenceNumbers(ImmutableSet.of(
                WithSeq.of(1, CLIENT_1),
                WithSeq.of(2, CLIENT_2)));
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

    private static PaxosProposal proposal(PaxosProposalId proposalId) {
        return new PaxosProposal(proposalId, paxosValue());
    }

    private static PaxosValue paxosValue() {
        return new PaxosValue(UUID.randomUUID().toString(), new Random().nextLong(), null);
    }

    private static AcceptorCacheDigest digest() {
        return ImmutableAcceptorCacheDigest.builder()
                .newCacheKey(UUID.randomUUID())
                .putUpdates(CLIENT_1, 50L)
                .build();
    }
}
