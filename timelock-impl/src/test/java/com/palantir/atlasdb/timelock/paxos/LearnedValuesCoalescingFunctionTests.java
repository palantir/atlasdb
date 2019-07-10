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
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
import com.palantir.atlasdb.timelock.paxos.PaxosQuorumCheckingCoalescingFunction.PaxosContainer;
import com.palantir.paxos.PaxosValue;

@RunWith(MockitoJUnitRunner.class)
public class LearnedValuesCoalescingFunctionTests {

    private static final Client CLIENT_1 = Client.of("client-1");
    private static final Client CLIENT_2 = Client.of("client-2");

    @Mock
    private BatchPaxosLearner remote;

    @Test
    public void canProcessBatch() {
        Set<WithSeq<Client>> remoteRequest = ImmutableSet.of(
                WithSeq.of(10, CLIENT_1),
                WithSeq.of(12, CLIENT_1),
                WithSeq.of(10, CLIENT_2));

        PaxosValue paxosValue1 = paxosValue(10);
        PaxosValue paxosValue2 = paxosValue(12);

        SetMultimap<Client, PaxosValue> remoteResponse = ImmutableSetMultimap.<Client, PaxosValue>builder()
                .putAll(CLIENT_1, paxosValue1, paxosValue2)
                .put(CLIENT_2, paxosValue1)
                .build();

        when(remote.getLearnedValues(remoteRequest))
                .thenReturn(remoteResponse);

        LearnedValuesCoalescingFunction function = new LearnedValuesCoalescingFunction(remote);
        Map<WithSeq<Client>, PaxosContainer<Optional<PaxosValue>>> results = function.apply(remoteRequest);

        assertThat(results)
                .containsEntry(WithSeq.of(10, CLIENT_1), asResult(paxosValue1))
                .containsEntry(WithSeq.of(12, CLIENT_1), asResult(paxosValue2))
                .containsEntry(WithSeq.of(10, CLIENT_2), asResult(paxosValue1));
    }

    private static PaxosValue paxosValue(long round) {
        return new PaxosValue(UUID.randomUUID().toString(), round, null);
    }

    private static PaxosContainer<Optional<PaxosValue>> asResult(PaxosValue value) {
        return PaxosContainer.of(Optional.of(value));
    }
}
